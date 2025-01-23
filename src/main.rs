use tokio::{net::{TcpListener, TcpStream}, io::{AsyncReadExt, AsyncWriteExt}};
use serde::{Serialize, Deserialize};
use std::{process::Command, path::PathBuf, collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, RwLock};
use tokio::time::sleep;
use local_ip_address::local_ip;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
struct Node {
    host: String,
    port: u16,
    cores: usize,
}

#[derive(Serialize, Deserialize, Debug)]
enum ClusterMessage {
    Discovery { node: Node },
    NodeList { nodes: Vec<Node> },
    Heartbeat { node: Node },
}

#[derive(Serialize, Deserialize, Debug)]
enum BuildRequest {
    StartBuild { 
        package_name: String,
        release: bool 
    },
    UploadSource {
        path: PathBuf,
        data: Vec<u8>
    },
    DownloadArtifact {
        path: PathBuf
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum BuildResponse {
    BuildStarted,
    BuildComplete { success: bool },
    BuildError { message: String },
    ArtifactData { data: Vec<u8> }
}

struct BuildCluster {
    local_node: Node,
    nodes: Arc<RwLock<HashSet<Node>>>,
    build_dir: PathBuf,
}

impl BuildCluster {
    fn new(port: u16, build_dir: PathBuf) -> Self {
        let local_ip = local_ip().unwrap().to_string();
        let local_node = Node {
            host: local_ip,
            port,
            cores: num_cpus::get(),
        };

        Self {
            local_node,
            nodes: Arc::new(RwLock::new(HashSet::new())),
            build_dir,
        }
    }

    async fn join_cluster(&self, seed_node: Option<String>) {
        if let Some(seed) = seed_node {
            let mut stream = TcpStream::connect(&seed).await.unwrap();
            let msg = ClusterMessage::Discovery {
                node: self.local_node.clone(),
            };
            let data = bincode::serialize(&msg).unwrap();
            stream.write_all(&data).await.unwrap();
        }

        // Start discovery listener
        let nodes = self.nodes.clone();
        let local_node = self.local_node.clone();
        
        tokio::spawn(async move {
            let listener = TcpListener::bind(format!("0.0.0.0:{}", local_node.port + 1)).await.unwrap();
            
            while let Ok((mut socket, _)) = listener.accept().await {
                let nodes = nodes.clone();
                let local_node = local_node.clone();
                
                tokio::spawn(async move {
                    let mut buf = vec![0; 1024];
                    if let Ok(n) = socket.read(&mut buf).await {
                        if let Ok(msg) = bincode::deserialize(&buf[..n]) {
                            match msg {
                                ClusterMessage::Discovery { node } => {
                                    nodes.write().await.insert(node);
                                    let response = ClusterMessage::NodeList {
                                        nodes: nodes.read().await.iter().cloned().collect(),
                                    };
                                    let data = bincode::serialize(&response).unwrap();
                                    socket.write_all(&data).await.unwrap();
                                },
                                ClusterMessage::NodeList { nodes: node_list } => {
                                    let mut cluster_nodes = nodes.write().await;
                                    for node in node_list {
                                        cluster_nodes.insert(node);
                                    }
                                },
                                ClusterMessage::Heartbeat { node } => {
                                    nodes.write().await.insert(node);
                                }
                            }
                        }
                    }
                });
            }
        });

        // Start heartbeat sender
        let nodes = self.nodes.clone();
        let local_node = self.local_node.clone();
        
        tokio::spawn(async move {
            loop {
                let node_list = nodes.read().await.iter().cloned().collect::<Vec<_>>();
                for node in node_list {
                    if node.host != local_node.host || node.port != local_node.port {
                        let msg = ClusterMessage::Heartbeat {
                            node: local_node.clone(),
                        };
                        let data = bincode::serialize(&msg).unwrap();
                        if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", node.host, node.port + 1)).await {
                            let _ = stream.write_all(&data).await;
                        }
                    }
                }
                sleep(Duration::from_secs(5)).await;
            }
        });
    }

    async fn handle_build(&self, package_name: String, release: bool) -> BuildResponse {
        // Find least loaded node
        let nodes = self.nodes.read().await;
        let target_node = nodes.iter()
            .min_by_key(|node| node.cores)
            .unwrap_or(&self.local_node);

        if target_node.host == self.local_node.host && target_node.port == self.local_node.port {
            // Build locally
            let target_dir = self.build_dir.join(&package_name);
            match Command::new("cargo")
                .current_dir(&target_dir)
                .arg("build")
                .args(if release { vec!["build", "--release"] } else { vec!["build"] })
                .status() 
            {
                Ok(status) => BuildResponse::BuildComplete { success: status.success() },
                Err(e) => BuildResponse::BuildError { message: e.to_string() }
            }
        } else {
            // Forward to selected node
            let mut stream = TcpStream::connect(format!("{}:{}", target_node.host, target_node.port)).await.unwrap();
            let request = BuildRequest::StartBuild { package_name, release };
            let data = bincode::serialize(&request).unwrap();
            stream.write_all(&data).await.unwrap();
            
            let mut buf = vec![0; 1024 * 1024];
            let n = stream.read(&mut buf).await.unwrap();
            bincode::deserialize(&buf[..n]).unwrap()
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port = args.get(1).map(|p| p.parse().unwrap()).unwrap_or(9876);
    let seed = args.get(2).cloned();

    let cluster = Arc::new(BuildCluster::new(
        port,
        PathBuf::from("/tmp/tess-builds")
    ));

    cluster.join_cluster(seed).await;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
    println!("Node listening on port {}", port);

    while let Ok((socket, addr)) = listener.accept().await {
        println!("New connection from {}", addr);
        let cluster = cluster.clone();
        
        tokio::spawn(async move {
            handle_connection(socket, cluster).await;
        });
    }
}

async fn handle_connection(mut socket: TcpStream, cluster: Arc<BuildCluster>) {
    let mut buf = vec![0; 1024 * 1024];
    
    while let Ok(n) = socket.read(&mut buf).await {
        if n == 0 { break; }
        
        if let Ok(request) = bincode::deserialize(&buf[..n]) {
            let response = match request {
                BuildRequest::StartBuild { package_name, release } => {
                    cluster.handle_build(package_name, release).await
                },
                // File operations handled similarly to previous version
                BuildRequest::UploadSource { path, data } => {
                    let target = cluster.build_dir.join(path);
                    if let Some(parent) = target.parent() {
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                    match tokio::fs::write(target, data).await {
                        Ok(_) => BuildResponse::BuildStarted,
                        Err(e) => BuildResponse::BuildError { message: e.to_string() }
                    }
                },
                BuildRequest::DownloadArtifact { path } => {
                    let file_path = cluster.build_dir.join(path);
                    match tokio::fs::read(&file_path).await {
                        Ok(data) => BuildResponse::ArtifactData { data },
                        Err(e) => BuildResponse::BuildError { message: e.to_string() }
                    }
                }
            };

            if let Ok(response_data) = bincode::serialize(&response) {
                let _ = socket.write_all(&response_data).await;
            }
        }
    }
}
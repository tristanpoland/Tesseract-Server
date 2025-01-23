use cargo_metadata::MetadataCommand;
use petgraph::{Graph, Directed};
use serde::{Serialize, Deserialize};
use tokio::{
    net::{TcpListener, TcpStream}, 
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
    time::{sleep, Duration}
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    process::Command
};
use tracing::{info, warn, error};
use futures::future::join_all;
use walkdir::WalkDir;

const BUFFER_SIZE: usize = 1024 * 1024 * 10;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
struct Node {
    host: String,
    port: u16,
    cores: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BuildUnit {
    package_name: String,
    dependencies: Vec<String>,
    source_files: Vec<PathBuf>,
    artifacts: Vec<PathBuf>,
}

#[derive(Serialize, Deserialize, Debug)]
enum BuildRequest {
    BuildUnit {
        unit: BuildUnit,
        release: bool,
    },
    TransferArtifact {
        from_unit: String,
        artifact_path: PathBuf,
    },
    Heartbeat
}

#[derive(Serialize, Deserialize, Debug)]
enum BuildResponse {
    BuildComplete {
        unit_name: String,
        artifacts: Vec<(PathBuf, Vec<u8>)>,
    },
    BuildError {
        unit_name: String,
        error: String,
    },
    HeartbeatAck
}

struct BuildCluster {
    local_node: Node,
    nodes: Arc<RwLock<HashMap<Node, Duration>>>,
    build_cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    build_dir: PathBuf,
}

impl BuildCluster {
    fn new(port: u16, build_dir: PathBuf) -> Self {
        let cores = num_cpus::get();
        let local_node = Node {
            host: local_ip_address::local_ip().unwrap().to_string(),
            port,
            cores,
        };

        Self {
            local_node: local_node.clone(),
            nodes: Arc::new(RwLock::new(HashMap::from([(local_node, tokio::time::Instant::now().elapsed())]))),
            build_cache: Arc::new(RwLock::new(HashMap::new())),
            build_dir,
        }
    }

    async fn join_cluster(&self, seed_node: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(seed) = seed_node {
            let mut stream = TcpStream::connect(&seed).await?;
            stream.set_nodelay(true)?;
            
            let msg = BuildRequest::Heartbeat;
            let data = bincode::serialize(&msg)?;
            let len = (data.len() as u32).to_be_bytes();
            stream.write_all(&len).await?;
            stream.write_all(&data).await?;

            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf);
            
            let mut buf = vec![0; len as usize];
            stream.read_exact(&mut buf).await?;
            
            if let Ok(BuildResponse::HeartbeatAck) = bincode::deserialize(&buf) {
                let mut nodes = self.nodes.write().await;
                nodes.insert(Node {
                    host: seed.split(':').next().unwrap().to_string(),
                    port: seed.split(':').nth(1).unwrap().parse()?,
                    cores: 0,
                }, tokio::time::Instant::now().elapsed());
            }
        }

        self.start_heartbeat().await;
        Ok(())
    }

    async fn start_heartbeat(&self) {
        let nodes = self.nodes.clone();
        let local_node = self.local_node.clone();

        tokio::spawn(async move {
            loop {
                let current_time = tokio::time::Instant::now();
                let mut nodes = nodes.write().await;
                
                nodes.retain(|_, last_seen| 
                    current_time.elapsed() - *last_seen < CONNECTION_TIMEOUT
                );

                for (node, _) in nodes.clone() {
                    if node != local_node {
                        if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", node.host, node.port)).await {
                            let msg = BuildRequest::Heartbeat;
                            if let Ok(data) = bincode::serialize(&msg) {
                                let len = (data.len() as u32).to_be_bytes();
                                let _ = stream.write_all(&len).await;
                                let _ = stream.write_all(&data).await;
                            }
                        }
                    }
                }

                drop(nodes);
                sleep(HEARTBEAT_INTERVAL).await;
            }
        });
    }

    async fn distribute_build(&self, workspace_path: PathBuf, release: bool) -> Result<(), Box<dyn std::error::Error>> {
        let metadata = MetadataCommand::new()
            .current_dir(&workspace_path)
            .exec()?;

        let mut graph = Graph::<BuildUnit, (), Directed>::new();
        let mut pkg_to_node = HashMap::new();

        for package in &metadata.packages {
            let source_files = package.targets.iter()
                .filter(|t| t.kind.iter().any(|k| k == "lib" || k == "bin"))
                .flat_map(|t| {
                    WalkDir::new(t.src_path.parent().unwrap())
                        .into_iter()
                        .filter_map(|e| e.ok())
                        .filter(|e| e.path().extension().map_or(false, |ext| ext == "rs"))
                        .map(|e| e.path().to_path_buf())
                })
                .collect();

            let unit = BuildUnit {
                package_name: package.name.clone(),
                dependencies: package.dependencies.iter()
                    .map(|d| d.name.clone())
                    .collect(),
                source_files,
                artifacts: package.targets.iter()
                    .map(|t| PathBuf::from(&t.name))
                    .collect(),
            };

            let idx = graph.add_node(unit);
            pkg_to_node.insert(package.name.clone(), idx);
        }

        for package in &metadata.packages {
            let from = pkg_to_node[&package.name];
            for dep in &package.dependencies {
                if let Some(&to) = pkg_to_node.get(&dep.name) {
                    graph.add_edge(from, to, ());
                }
            }
        }

        let build_order = petgraph::algo::toposort(&graph, None)
            .map_err(|_| "Cyclic dependencies detected")?;

        let mut futures = Vec::new();
        let nodes = self.nodes.read().await;
        let mut node_iter = nodes.keys().cycle();

        for unit_idx in build_order {
            let unit = graph[unit_idx].clone();
            if let Some(node) = node_iter.next() {
                let node = node.clone();
                let self_ref = self.clone();
                let build_future = async move {
                    self_ref.build_on_node(&node, unit, release).await
                };
                futures.push(build_future);
            }
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(BuildResponse::BuildComplete { unit_name, artifacts }) => {
                    info!("Build complete for {}", unit_name);
                    let mut cache = self.build_cache.write().await;
                    for (path, data) in artifacts {
                        cache.insert(format!("{}:{}", unit_name, path.display()), data);
                    }
                }
                Ok(BuildResponse::BuildError { unit_name, error }) => {
                    error!("Build failed for {}: {}", unit_name, error);
                    return Err(format!("Build failed for {}: {}", unit_name, error).into());
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn build_on_node(
        &self, 
        node: &Node,
        unit: BuildUnit,
        release: bool
    ) -> Result<BuildResponse, Box<dyn std::error::Error>> {
        if node == &self.local_node {
            return self.build_locally(unit, release).await;
        }

        let mut stream = TcpStream::connect(format!("{}:{}", node.host, node.port)).await?;
        stream.set_nodelay(true)?;

        let req = BuildRequest::BuildUnit { unit, release };
        let data = bincode::serialize(&req)?;
        let len = (data.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&data).await?;

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf);
        
        let mut buf = vec![0; len as usize];
        stream.read_exact(&mut buf).await?;
        let resp: BuildResponse = bincode::deserialize(&buf)?;

        Ok(resp)
    }

    async fn build_locally(&self, unit: BuildUnit, release: bool) -> Result<BuildResponse, Box<dyn std::error::Error>> {
        let package_dir = self.build_dir.join(&unit.package_name);
        
        // Directory creation error
        if let Err(e) = tokio::fs::create_dir_all(&package_dir).await {
            return Ok(BuildResponse::BuildError {
                unit_name: unit.package_name.clone(),
                error: format!("Failed to create build directory: {}", e),
            });
        }
    
        // Source file copy errors
        for source_path in &unit.source_files {
            if !source_path.exists() {
                return Ok(BuildResponse::BuildError {
                    unit_name: unit.package_name.clone(),
                    error: format!("Source file not found: {}", source_path.display()),
                });
            }
    
            let target_path = package_dir.join(source_path);
            if let Some(parent) = target_path.parent() {
                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                    return Ok(BuildResponse::BuildError {
                        unit_name: unit.package_name.clone(),
                        error: format!("Failed to create directory for {}: {}", source_path.display(), e),
                    });
                }
            }
            
            match tokio::fs::read(source_path).await {
                Ok(content) => {
                    if let Err(e) = tokio::fs::write(&target_path, content).await {
                        return Ok(BuildResponse::BuildError {
                            unit_name: unit.package_name.clone(),
                            error: format!("Failed to write {}: {}", target_path.display(), e),
                        });
                    }
                }
                Err(e) => {
                    return Ok(BuildResponse::BuildError {
                        unit_name: unit.package_name.clone(),
                        error: format!("Failed to read source file {}: {}", source_path.display(), e),
                    });
                }
            }
        }
    
        // Cargo build errors
        let output = Command::new("cargo")
            .current_dir(&package_dir)
            .arg("build")
            .args(if release { vec!["--release"] } else { vec![] })
            .output()
            .map_err(|e| format!("Failed to execute cargo build: {}", e))?;
    
        if !output.status.success() {
            return Ok(BuildResponse::BuildError {
                unit_name: unit.package_name,
                error: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }
    
        // Artifact collection errors
        let mut artifacts = Vec::new();
        let target_dir = package_dir.join("target").join(if release { "release" } else { "debug" });
        
        for artifact_path in &unit.artifacts {
            let file_path = target_dir.join(artifact_path);
            match tokio::fs::read(&file_path).await {
                Ok(data) => artifacts.push((artifact_path.clone(), data)),
                Err(e) => {
                    return Ok(BuildResponse::BuildError {
                        unit_name: unit.package_name,
                        error: format!("Failed to read artifact {}: {}", file_path.display(), e),
                    });
                }
            }
        }
    
        Ok(BuildResponse::BuildComplete {
            unit_name: unit.package_name,
            artifacts,
        })
    }
}

async fn handle_connection(mut socket: TcpStream, cluster: Arc<BuildCluster>) {
    socket.set_nodelay(true).unwrap();
    
    loop {
        let mut len_buf = [0u8; 4];
        if socket.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        
        let mut buf = vec![0; len];
        if socket.read_exact(&mut buf).await.is_err() {
            break;
        }
        
        if let Ok(request) = bincode::deserialize(&buf) {
            let response = match request {
                BuildRequest::BuildUnit { unit, release } => {
                    match cluster.build_locally(unit, release).await {
                        Ok(response) => response,
                        Err(e) => BuildResponse::BuildError {
                            unit_name: "unknown".to_string(),
                            error: e.to_string(),
                        }
                    }
                },
                BuildRequest::TransferArtifact { from_unit, artifact_path } => {
                    let cache = cluster.build_cache.read().await;
                    if let Some(data) = cache.get(&format!("{}:{}", from_unit, artifact_path.display())) {
                        BuildResponse::BuildComplete {
                            unit_name: from_unit,
                            artifacts: vec![(artifact_path, data.clone())]
                        }
                    } else {
                        BuildResponse::BuildError {
                            unit_name: from_unit,
                            error: "Artifact not found".to_string()
                        }
                    }
                },
                BuildRequest::Heartbeat => {
                    BuildResponse::HeartbeatAck
                }
            };

            if let Ok(response_data) = bincode::serialize(&response) {
                let len = (response_data.len() as u32).to_be_bytes();
                let _ = socket.write_all(&len).await;
                let _ = socket.write_all(&response_data).await;
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    let port = args.get(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(9876);
    let seed = args.get(2).cloned();

    let build_dir = std::env::current_dir()?.join("builds");
    tokio::fs::create_dir_all(&build_dir).await?;

    let cluster = Arc::new(BuildCluster::new(port, build_dir));
    cluster.join_cluster(seed).await?;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("Node listening on port {}", port);

    while let Ok((socket, addr)) = listener.accept().await {
        info!("New connection from {}", addr);
        let cluster = cluster.clone();
        
        tokio::spawn(async move {
            handle_connection(socket, cluster).await;
        });
    }

    Ok(())
}
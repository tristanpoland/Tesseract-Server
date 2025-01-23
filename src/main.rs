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
use futures::{future::join_all, FutureExt};
use walkdir::WalkDir;

const BUFFER_SIZE: usize = 1024 * 1024 * 10; // 10MB buffer for large builds
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
    nodes: Arc<RwLock<HashMap<Node, tokio::time::Instant>>>,
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
            nodes: Arc::new(RwLock::new(HashMap::from([(local_node, tokio::time::Instant::now())]))),
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
            stream.write_all(&data).await?;

            let mut buf = vec![0; BUFFER_SIZE];
            let n = stream.read(&mut buf).await?;
            
            if let Ok(BuildResponse::HeartbeatAck) = bincode::deserialize(&buf[..n]) {
                let mut nodes = self.nodes.write().await;
                nodes.insert(Node {
                    host: seed.split(':').next().unwrap().to_string(),
                    port: seed.split(':').nth(1).unwrap().parse()?,
                    cores: 0, // Will be updated with heartbeat
                }, tokio::time::Instant::now());
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
                
                // Remove stale nodes
                nodes.retain(|_, last_seen| 
                    current_time.duration_since(*last_seen) < CONNECTION_TIMEOUT
                );

                // Send heartbeat to all nodes
                for (node, _) in nodes.clone() {
                    if node != local_node {
                        if let Ok(mut stream) = TcpStream::connect(format!("{}:{}", node.host, node.port)).await {
                            let msg = BuildRequest::Heartbeat;
                            if let Ok(data) = bincode::serialize(&msg) {
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

        // Create build units
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

        // Add dependency edges
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

        // Start builds in dependency order
        for unit_idx in build_order {
            let unit = graph[unit_idx].clone();
            if let Some(node) = node_iter.next() {
                let node_clone = node.clone();
                let build_future = {
                    let node_owned = node_clone.clone();
                    async move {
                        self.build_on_node(&node_owned, unit, release).await
                    }
                };
                futures.push(build_future);
            }
        }

        // Wait for all builds
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
        stream.write_all(&data).await?;

        let mut buf = vec![0; BUFFER_SIZE];
        let n = stream.read(&mut buf).await?;
        let resp: BuildResponse = bincode::deserialize(&buf[..n])?;

        Ok(resp)
    }

    async fn build_locally(&self, unit: BuildUnit, release: bool) -> Result<BuildResponse, Box<dyn std::error::Error>> {
        let package_dir = self.build_dir.join(&unit.package_name);
        tokio::fs::create_dir_all(&package_dir).await?;

        // Write source files
        for source_path in &unit.source_files {
            let target_path = package_dir.join(source_path);
            if let Some(parent) = target_path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if let Ok(content) = tokio::fs::read(source_path).await {
                tokio::fs::write(&target_path, content).await?;
            }
        }

        // Build
        let status = Command::new("cargo")
            .current_dir(&package_dir)
            .arg("build")
            .args(if release { vec!["--release"] } else { vec![] })
            .status()?;

        if status.success() {
            // Collect artifacts
            let mut artifacts = Vec::new();
            let target_dir = package_dir.join("target").join(if release { "release" } else { "debug" });
            
            for artifact_path in &unit.artifacts {
                let file_path = target_dir.join(artifact_path);
                if let Ok(data) = tokio::fs::read(&file_path).await {
                    artifacts.push((artifact_path.clone(), data));
                }
            }

            Ok(BuildResponse::BuildComplete {
                unit_name: unit.package_name,
                artifacts,
            })
        } else {
            Ok(BuildResponse::BuildError {
                unit_name: unit.package_name,
                error: "Build failed".to_string(),
            })
        }
    }
}

async fn handle_connection(mut socket: TcpStream, cluster: Arc<BuildCluster>) {
    socket.set_nodelay(true).unwrap();
    let mut buf = vec![0; BUFFER_SIZE];
    
    while let Ok(n) = socket.read(&mut buf).await {
        if n == 0 { break; }
        
        if let Ok(request) = bincode::deserialize(&buf[..n]) {
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
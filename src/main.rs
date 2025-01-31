use anyhow::{Context, Result};
use cargo_metadata::MetadataCommand;
use clap::Parser;
use flate2::{Compression, write::GzEncoder, read::GzDecoder};
use futures::{future::join_all, StreamExt};
use local_ip_address::local_ip;
use petgraph::{Graph, Directed};
use serde::{Serialize, Deserialize};
use walkdir::WalkDir;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration
};
use tar::Archive;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, AsyncBufReadExt}, 
    net::{TcpListener, TcpStream}, 
    sync::{mpsc, RwLock}, 
    time,
    process::Command as TokioCommand,
};
use tracing::{error, info, warn, debug, instrument, Level};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    #[arg(short, long, default_value_t = 9876)]
    port: u16,

    #[arg(short, long)]
    seed: Option<String>,

    #[arg(short = 'j', long, default_value_t = 4)]
    max_jobs: usize,

    #[arg(short, long, default_value_t = 600)]
    timeout: u64,

    #[arg(short, long)]
    debug: bool,
}

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
        target: Option<String>,
        tarball_data: Vec<u8> 
    },
    TransferArtifact {
        from_unit: String,
        artifact_path: PathBuf,
    },
    Heartbeat
}

#[derive(Serialize, Deserialize, Debug)]
enum BuildResponse {
    BuildOutput {
        unit_name: String,
        output: String,
        is_error: bool,
    },
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

#[derive(Clone)]
struct BuildConfig {
    port: u16,
    seed: Option<String>,
    max_jobs: usize,
    timeout: Duration,
}

struct BuildCluster {
    config: Arc<BuildConfig>,
    local_node: Node,
    nodes: Arc<RwLock<HashMap<Node, time::Instant>>>,
    build_cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    build_dir: PathBuf,
}

impl BuildCluster {
    fn new(config: Arc<BuildConfig>, build_dir: PathBuf) -> Self {
        let cores = num_cpus::get();
        let local_node = Node {
            host: local_ip().unwrap().to_string(),
            port: config.port,
            cores,
        };

        Self {
            config,
            local_node: local_node.clone(),
            nodes: Arc::new(RwLock::new(HashMap::from([(local_node, time::Instant::now())]))),
            build_cache: Arc::new(RwLock::new(HashMap::new())),
            build_dir,
        }
    }

    async fn send_build_response(
        stream: &mut TcpStream,
        response: BuildResponse,
    ) -> Result<()> {
        let response_data = bincode::serialize(&response)?;
        let len = (response_data.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&response_data).await?;
        stream.flush().await?;
        Ok(())
    }

    async fn ensure_target_installed(target: &str) -> Result<()> {
        let output = TokioCommand::new("rustup")
            .args(["target", "list", "--installed"])
            .output()
            .await?;

        let installed_targets = String::from_utf8_lossy(&output.stdout);
        if !installed_targets.lines().any(|line| line.trim() == target) {
            info!("Installing target {}", target);
            let install_output = TokioCommand::new("rustup")
                .args(["target", "add", target])
                .output()
                .await?;

            if !install_output.status.success() {
                let error = String::from_utf8_lossy(&install_output.stderr);
                return Err(anyhow::anyhow!("Failed to install target {}: {}", target, error));
            }
        }
        Ok(())
    }

    fn extract_tarball(tarball_data: &[u8], dest_path: &Path) -> Result<()> {
        std::fs::create_dir_all(dest_path)?;
        let decoder = GzDecoder::new(tarball_data);
        let mut archive = Archive::new(decoder);

        // First, extract all files
        archive.unpack(dest_path)?;

        // Verify the expected files exist
        let workspace_toml = dest_path.join("Cargo.toml");
        if !workspace_toml.exists() {
            return Err(anyhow::anyhow!("Workspace Cargo.toml not found after extraction"));
        }

        Ok(())
    }

    #[instrument(skip(self))]
    async fn build_locally_with_tarball(
        &self,
        stream: &mut TcpStream,
        unit: BuildUnit,
        release: bool,
        target: Option<String>,
        tarball_data: Vec<u8>
    ) -> Result<BuildResponse> {
        let package_name = unit.package_name.clone();
        
        // Install target if needed
        if let Some(target_triple) = &target {
            if let Err(e) = Self::ensure_target_installed(target_triple).await {
                return Ok(BuildResponse::BuildError {
                    unit_name: package_name,
                    error: format!("Failed to install target {}: {}", target_triple, e),
                });
            }
        }

        // Setup build directory
        let package_dir = self.build_dir.join(format!("{}_{}", package_name, std::process::id()));
        let _ = tokio::fs::remove_dir_all(&package_dir).await;
        tokio::fs::create_dir_all(&package_dir).await?;

        info!(dir = %package_dir.display(), "Extracting source tarball");
        Self::extract_tarball(&tarball_data, &package_dir)
            .context("Failed to extract source tarball")?;

        let cargo_toml_path = package_dir.join("Cargo.toml");
        if !cargo_toml_path.exists() {
            return Ok(BuildResponse::BuildError {
                unit_name: package_name,
                error: "Cargo.toml not found in extracted tarball".to_string(),
            });
        }

        // Setup build command
        let mut cmd = TokioCommand::new("cargo");
        cmd.current_dir(&package_dir)
            .arg("build")
            .args(if release { vec!["--release"] } else { vec![] });

        if let Some(target_triple) = &target {
            cmd.args(["--target", target_triple]);
        }

        // Execute build with output streaming
        let mut child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to execute cargo build")?;

        let stdout = child.stdout.take().expect("Failed to capture stdout");
        let stderr = child.stderr.take().expect("Failed to capture stderr");
        
        let mut stdout_reader = BufReader::new(stdout).lines();
        let mut stderr_reader = BufReader::new(stderr).lines();
        
        let mut build_output = Vec::new();
        let mut build_failed = false;

        // Stream output
        loop {
            tokio::select! {
                Ok(Some(line)) = stdout_reader.next_line() => {
                    if !line.trim().is_empty() {
                        info!(unit_name = %package_name, "cargo stdout: {}", line);
                        Self::send_build_response(
                            stream,
                            BuildResponse::BuildOutput {
                                unit_name: package_name.clone(),
                                output: line.clone(),
                                is_error: false,
                            }
                        ).await?;
                        build_output.push(line);
                    }
                }
                Ok(Some(line)) = stderr_reader.next_line() => {
                    if !line.trim().is_empty() {
                        warn!(unit_name = %package_name, "cargo stderr: {}", line);
                        Self::send_build_response(
                            stream,
                            BuildResponse::BuildOutput {
                                unit_name: package_name.clone(),
                                output: line.clone(),
                                is_error: true,
                            }
                        ).await?;
                        build_output.push(line);
                    }
                }
                result = child.wait() => {
                    match result {
                        Ok(status) => {
                            if !status.success() {
                                build_failed = true;
                            }
                            break;
                        }
                        Err(e) => {
                            return Ok(BuildResponse::BuildError {
                                unit_name: package_name,
                                error: format!("Build process error: {}", e),
                            });
                        }
                    }
                }
            }
        }

        if build_failed {
            let error_msg = build_output.join("\n");
            return Ok(BuildResponse::BuildError {
                unit_name: package_name,
                error: error_msg,
            });
        }

        // Collect artifacts
        let mut artifacts = Vec::new();
        let target_dir = package_dir.join("target");

        // Function to check multiple possible paths
        let check_artifact_paths = |base_name: &Path| -> Vec<PathBuf> {
            let mut paths = Vec::new();
            
            let file_name = base_name.file_name().unwrap().to_str().unwrap();
            
            // Release or debug directory path
            let mut binary_dir = if let Some(target_triple) = &target {
                target_dir.join(target_triple)
            } else {
                target_dir.clone()
            };
            binary_dir = binary_dir.join(if release { "release" } else { "debug" });

            // Check with and without .exe extension
            paths.push(binary_dir.join(file_name));
            paths.push(binary_dir.join(format!("{}.exe", file_name)));
            
            paths
        };

        info!("Looking for artifacts in: {}", target_dir.display());

        for artifact_name in &unit.artifacts {
            let mut found = false;
            let possible_paths = check_artifact_paths(artifact_name);
            
            for path in &possible_paths {
                info!("Checking path: {}", path.display());
                match tokio::fs::read(&path).await {
                    Ok(data) => {
                        info!(
                            "Found artifact at {}, size: {} bytes",
                            path.display(),
                            data.len()
                        );
                        artifacts.push((artifact_name.clone(), data));
                        found = true;
                        break;
                    }
                    Err(e) => {
                        debug!(
                            "Artifact not found at {}: {}",
                            path.display(),
                            e
                        );
                    }
                }
            }

            if !found {
                let paths_tried = possible_paths
                    .iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                    
                return Ok(BuildResponse::BuildError {
                    unit_name: package_name,
                    error: format!(
                        "Could not find artifact '{}'. Tried paths: {}",
                        artifact_name.display(),
                        paths_tried
                    ),
                });
            }
        }

        // Clean up
        let cleanup_dir = package_dir.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::fs::remove_dir_all(&cleanup_dir).await {
                warn!(dir = %cleanup_dir.display(), error = %e, "Failed to clean up build directory");
            }
        });

        info!(
            unit_name = %package_name,
            artifact_count = %artifacts.len(),
            "Build and artifact collection complete"
        );

        Ok(BuildResponse::BuildComplete {
            unit_name: package_name,
            artifacts,
        })
    }

    async fn handle_request(
        &self,
        stream: &mut TcpStream,
        request: BuildRequest
    ) -> Result<BuildResponse> {
        match request {
            BuildRequest::BuildUnit { unit, release, target, tarball_data } => {
                self.build_locally_with_tarball(stream, unit, release, target, tarball_data).await
            }
            BuildRequest::TransferArtifact { from_unit, artifact_path } => {
                let cache = self.build_cache.read().await;
                match cache.get(&format!("{}:{}", from_unit, artifact_path.display())) {
                    Some(data) => Ok(BuildResponse::BuildComplete {
                        unit_name: from_unit,
                        artifacts: vec![(artifact_path, data.clone())]
                    }),
                    None => Ok(BuildResponse::BuildError {
                        unit_name: from_unit,
                        error: "Artifact not found".to_string()
                    })
                }
            }
            BuildRequest::Heartbeat => Ok(BuildResponse::HeartbeatAck)
        }
    }

    async fn start_heartbeat(&self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(100);
        
        tokio::spawn({
            let nodes = self.nodes.clone();
            let local_node = self.local_node.clone();
            async move {
                while let Some(_) = rx.recv().await {
                    let current_time = time::Instant::now();
                    let mut nodes = nodes.write().await;
                    nodes.retain(|node, last_seen| 
                        *node == local_node || current_time.duration_since(*last_seen) < Duration::from_secs(30)
                    );
                }
            }
        });

        tokio::spawn({
            let nodes = self.nodes.clone();
            let local_node = self.local_node.clone();
            let tx = tx.clone();
            async move {
                loop {
                    let nodes = nodes.read().await;
                    for (node, _) in nodes.iter() {
                        if node != &local_node {
                            if let Err(e) = TcpStream::connect(format!("{}:{}", node.host, node.port)).await {
                                warn!("Heartbeat failed for {}:{}: {}", node.host, node.port, e);
                            }
                        }
                    }
                    let _ = tx.send(()).await;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

        Ok(())
    }
}

async fn handle_connection(mut socket: TcpStream, cluster: Arc<BuildCluster>) -> Result<()> {
    let peer_addr = socket.peer_addr()?;
    socket.set_nodelay(true)?;
    
    loop {
        let mut len_buf = [0u8; 4];
        match socket.read_exact(&mut len_buf).await {
            Ok(_) => (),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    info!(peer = %peer_addr, "Client disconnected");
                } else {
                    error!(peer = %peer_addr, error = %e, "Error reading message length");
                }
                return Ok(());
            }
        }
        
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 100_000_000 { // 100MB limit
            error!(peer = %peer_addr, length = len, "Request too large");
            return Ok(());
        }
        
        let mut buf = vec![0; len];
        if let Err(e) = socket.read_exact(&mut buf).await {
            error!(peer = %peer_addr, error = %e, "Error reading request data");
            return Ok(());
        }
        
        let request: BuildRequest = match bincode::deserialize(&buf) {
            Ok(req) => req,
            Err(e) => {
                error!(peer = %peer_addr, error = %e, "Deserialization error");
                continue;
            }
        };

        debug!(peer = %peer_addr, ?request, "Received request");

        let response = match cluster.handle_request(&mut socket, request).await {
            Ok(resp) => resp,
            Err(e) => BuildResponse::BuildError {
                unit_name: "unknown".to_string(),
                error: e.to_string(),
            }
        };

        let response_data = bincode::serialize(&response)?;
        let len = (response_data.len() as u32).to_be_bytes();

        let mut combined = Vec::with_capacity(4 + response_data.len());
        combined.extend_from_slice(&len);
        combined.extend_from_slice(&response_data);

        if let Err(e) = socket.write_all(&combined).await {
            error!(peer = %peer_addr, error = %e, "Error sending response");
            return Ok(());
        }

        socket.flush().await?;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();

    let log_level = if args.debug { Level::DEBUG } else { Level::INFO };
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let config = Arc::new(BuildConfig {
        port: args.port,
        seed: args.seed,
        max_jobs: args.max_jobs,
        timeout: Duration::from_secs(args.timeout),
    });

    let build_dir = std::env::current_dir()?.join("builds");
    tokio::fs::create_dir_all(&build_dir).await?;

    let cluster = Arc::new(BuildCluster::new(config.clone(), build_dir));

    if let Some(seed) = config.seed.clone() {
        info!(seed = %seed, "Attempting to join cluster");
        if let Err(e) = TcpStream::connect(&seed).await {
            warn!(error = %e, "Failed to join cluster");
        } else {
            info!("Successfully joined cluster");
        }
    }

    cluster.start_heartbeat().await?;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;
    info!(port = %config.port, "Build node listening");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!(client = %addr, "New connection");
                let cluster = cluster.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, cluster).await {
                        error!(error = %e, "Connection handling error");
                    }
                });
            }
            Err(e) => {
                error!(error = %e, "Failed to accept connection");
            }
        }
    }
}
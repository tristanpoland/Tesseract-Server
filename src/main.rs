use anyhow::{Context, Result};
use cargo_metadata::MetadataCommand;
use clap::Parser;
use flate2::{Compression, write::GzEncoder, read::GzDecoder};
use futures::{future::join_all, StreamExt};
use local_ip_address::local_ip;
use petgraph::{Graph, Directed};
use regex::Regex;
use serde::{Serialize, Deserialize};
use walkdir::WalkDir;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration
};
use tar::Archive;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, process::Command as TokioCommand, sync::{mpsc, RwLock}, time
};
use tracing::{error, info, warn, debug, instrument, Level};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct CliArgs {
    /// Port to listen on
    #[arg(short, long, default_value_t = 9876)]
    port: u16,

    /// Seed node to join (host:port)
    #[arg(short, long)]
    seed: Option<String>,

    /// Maximum concurrent build jobs
    #[arg(short = 'j', long, default_value_t = 4)]
    max_jobs: usize,

    /// Build timeout in seconds
    #[arg(short, long, default_value_t = 600)]
    timeout: u64,

    /// Enable debug logging
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
    config: Arc<BuildConfig>,
    local_node: Node,
    nodes: Arc<RwLock<HashMap<Node, time::Instant>>>,
    build_cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    build_dir: PathBuf,
}

#[derive(Clone)]
struct BuildConfig {
    port: u16,
    seed: Option<String>,
    max_jobs: usize,
    timeout: Duration,
}

fn get_possible_artifact_paths(target_dir: &Path, base_name: &Path, target_triple: Option<&str>) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let base_path = target_dir.join(base_name);
    
    // Start with the base name
    paths.push(base_path.clone());

    // Check if target indicates Windows
    let is_windows = target_triple
        .map(|t| t.contains("windows"))
        .unwrap_or(cfg!(windows));

    // Add Windows extension if appropriate
    if is_windows {
        paths.push(base_path.with_extension("exe"));
    }

    // Add Unix executable (no extension) if appropriate
    if !is_windows {
        paths.push(base_path.clone());
    }

    // Add dynamic library extensions
    match target_triple.map(|t| t.split('-').next().unwrap_or("")) {
        Some("windows") | Some("pc-windows") => {
            paths.push(base_path.with_extension("dll"));
        }
        Some("apple") | Some("ios") | Some("darwin") => {
            paths.push(base_path.with_extension("dylib"));
        }
        _ => {
            paths.push(base_path.with_extension("so"));
        }
    }

    // Always check for rlib (static library)
    paths.push(base_path.with_extension("rlib"));

    paths
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

    async fn ensure_target_installed(target: &str) -> Result<()> {
        // Check if target is already installed
        let output = TokioCommand::new("rustup")
            .args(["target", "list", "--installed"])
            .output()
            .await?;

        let installed_targets = String::from_utf8_lossy(&output.stdout);
        if !installed_targets.lines().any(|line| line.trim() == target) {
            // Install the target
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

    fn get_gitignore_patterns() -> Vec<String> {
        let mut patterns = vec![
            ".git".to_string(),
            "target".to_string(),
            "node_modules".to_string(),
            "Cargo.lock".to_string(),
        ];

        if let Ok(content) = std::fs::read_to_string(".gitignore") {
            patterns.extend(content.lines()
                .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
                .map(|line| line.trim().to_string()));
        }
        patterns
    }

    fn is_path_ignored(path: &Path, patterns: &[String]) -> bool {
        let path_str = path.to_string_lossy();
        patterns.iter().any(|pattern| {
            let pattern = pattern.trim_start_matches('/').trim_end_matches('/');
            if pattern.contains('*') {
                let regex = Self::glob_to_regex(pattern);
                regex.is_match(&path_str)
            } else {
                path_str.contains(pattern)
            }
        })
    }

    fn glob_to_regex(pattern: &str) -> Regex {
        let regex_pattern = pattern
            .replace(".", "\\.")
            .replace("**/", "(.*/)?")
            .replace("*", "[^/]*")
            .replace("?", ".");
        Regex::new(&format!("^{}$", regex_pattern))
            .unwrap_or_else(|_| Regex::new("^$").unwrap())
    }

    fn create_tarball(unit: &BuildUnit) -> Result<Vec<u8>> {
        let temp_dir = tempfile::tempdir()?;
        let temp_path = temp_dir.path();

        let patterns = Self::get_gitignore_patterns();
        let mut added_files = HashSet::new();

        for source_path in &unit.source_files {
            if source_path.exists() && !Self::is_path_ignored(source_path, &patterns) {
                let relative_path = source_path.strip_prefix(source_path.parent().unwrap())?;
                let dest_path = temp_path.join(relative_path);
                
                if let Some(parent) = dest_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                
                if !added_files.contains(&dest_path) {
                    std::fs::copy(source_path, &dest_path)?;
                    added_files.insert(dest_path);
                }
            }
        }
        
        let mut tarball = Vec::new();
        {
            let encoder = GzEncoder::new(&mut tarball, Compression::default());
            let mut tar = tar::Builder::new(encoder);
            tar.append_dir_all(".", temp_path)?;
            tar.finish()?;
        }
        
        Ok(tarball)
    }

    fn extract_tarball(tarball_data: &[u8], dest_path: &Path) -> Result<()> {
        std::fs::create_dir_all(dest_path)?;
        
        let decoder = GzDecoder::new(tarball_data);
        let mut archive = Archive::new(decoder);
        archive.unpack(dest_path)?;
        
        Ok(())
    }

    #[instrument(skip(self))]
    async fn build_locally_with_tarball(
        &self, 
        unit: BuildUnit, 
        release: bool,
        target: Option<String>,
        tarball_data: Vec<u8>
    ) -> Result<BuildResponse> {
        info!(
            unit_name = %unit.package_name,
            release = %release,
            target = ?target,
            "Starting build"
        );
    
        // Install target if specified
        if let Some(target_triple) = &target {
            info!(target = %target_triple, "Ensuring target is installed");
            if let Err(e) = Self::ensure_target_installed(target_triple).await {
                return Ok(BuildResponse::BuildError {
                    unit_name: unit.package_name.clone(),
                    error: format!("Failed to install target {}: {}", target_triple, e),
                });
            }
        }
    
        let package_dir = self.build_dir.join(format!("{}_{}", unit.package_name, std::process::id()));
        
        // Clean up previous builds
        if package_dir.exists() {
            info!(dir = %package_dir.display(), "Cleaning previous build directory");
            let _ = tokio::fs::remove_dir_all(&package_dir).await;
        }
        
        info!(dir = %package_dir.display(), "Creating build directory");
        tokio::fs::create_dir_all(&package_dir).await?;
    
        // Extract tarball 
        info!(unit_name = %unit.package_name, "Extracting source tarball");
        Self::extract_tarball(&tarball_data, &package_dir)
            .context("Failed to extract source tarball")?;
    
        // Validate Cargo.toml
        let cargo_toml_path = package_dir.join("Cargo.toml");
        if !cargo_toml_path.exists() {
            return Ok(BuildResponse::BuildError {
                unit_name: unit.package_name,
                error: "Cargo.toml not found in extracted tarball".to_string(),
            });
        }
    
        // Prepare build command
        let mut cmd = TokioCommand::new("cargo");
        cmd.current_dir(&package_dir)
            .arg("build")
            .args(if release { vec!["--release"] } else { vec![] });
    
        // Add target if specified
        if let Some(target_triple) = &target {
            cmd.args(["--target", target_triple]);
        }
    
        info!(
            unit_name = %unit.package_name,
            release = %release,
            target = ?target,
            "Starting cargo build"
        );
    
        // Execute build with live output streaming
        let mut child = cmd
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to execute cargo build")?;
    
        let stdout = child.stdout.take().expect("Failed to capture stdout");
        let stderr = child.stderr.take().expect("Failed to capture stderr");
        
        // Stream stdout and stderr in separate tasks
        let stdout_handle = tokio::spawn({
            let package_name = unit.package_name.clone();
            async move {
                let mut reader = tokio::io::BufReader::new(stdout);
                let mut line = String::new();
                let mut output = Vec::new();
                while let Ok(n) = reader.read_line(&mut line).await {
                    if n == 0 { break; }
                    info!(unit_name = %package_name, "cargo stdout: {}", line.trim());
                    output.push(line.clone());
                    line.clear();
                }
                output.join("")
            }
        });
    
        let stderr_handle = tokio::spawn({
            let package_name = unit.package_name.clone();
            async move {
                let mut reader = tokio::io::BufReader::new(stderr);
                let mut line = String::new();
                let mut output = Vec::new();
                while let Ok(n) = reader.read_line(&mut line).await {
                    if n == 0 { break; }
                    warn!(unit_name = %package_name, "cargo stderr: {}", line.trim());
                    output.push(line.clone());
                    line.clear();
                }
                output.join("")
            }
        });
    
        // Wait for build to complete and collect output
        let status = child.wait().await?;
        let stdout_output = stdout_handle.await.unwrap_or_default();
        let stderr_output = stderr_handle.await.unwrap_or_default();
        
        // Check build result
        if !status.success() {
            let error_msg = if stderr_output.is_empty() {
                stdout_output
            } else {
                stderr_output
            };
            error!(unit_name = %unit.package_name, "Build failed: {}", error_msg);
            return Ok(BuildResponse::BuildError {
                unit_name: unit.package_name,
                error: error_msg,
            });
        }
    
        info!(unit_name = %unit.package_name, "Build completed successfully");
    
        // Collect artifacts
        let mut artifacts = Vec::new();
        let mut target_dir = package_dir.join("target");
        if let Some(target_triple) = &target {
            target_dir = target_dir.join(target_triple);
        }
        target_dir = target_dir.join(if release { "release" } else { "debug" });
        
        info!(
            unit_name = %unit.package_name,
            target_dir = %target_dir.display(),
            "Collecting artifacts"
        );
    
        for base_artifact_path in &unit.artifacts {
            // Try multiple possible extensions based on target
            let possible_paths = get_possible_artifact_paths(&target_dir, base_artifact_path, target.as_deref());
            
            let mut found = false;
            for file_path in &possible_paths {
                match tokio::fs::read(&file_path).await {
                    Ok(data) => {
                        info!(
                            unit_name = %unit.package_name,
                            artifact = %file_path.display(),
                            size = %data.len(),
                            "Artifact collected"
                        );
                        // Use the actual filename as the artifact path
                        let final_name = file_path.file_name()
                            .and_then(|n| n.to_str())
                            .map(|n| PathBuf::from(n))
                            .unwrap_or_else(|| base_artifact_path.clone());
                        artifacts.push((final_name, data));
                        found = true;
                        break;
                    }
                    Err(_) => continue,
                }
            }
            
            if !found {
                let paths_tried = possible_paths.iter()
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                
                error!(
                    unit_name = %unit.package_name,
                    artifact = %base_artifact_path.display(),
                    tried_paths = %paths_tried,
                    "Failed to find artifact in any platform-specific form"
                );
                return Ok(BuildResponse::BuildError {
                    unit_name: unit.package_name,
                    error: format!("Failed to find artifact {}. Tried paths: {}", base_artifact_path.display(), paths_tried),
                });
            }
        }
    
        // // Background cleanup
        // let cleanup_dir = package_dir.clone();
        // tokio::spawn(async move {
        //     info!(dir = %cleanup_dir.display(), "Cleaning up build directory");
        //     if let Err(e) = tokio::fs::remove_dir_all(&cleanup_dir).await {
        //         warn!(dir = %cleanup_dir.display(), error = %e, "Failed to clean up build directory");
        //     }
        // });
    
        info!(
            unit_name = %unit.package_name,
            artifact_count = %artifacts.len(),
            "Build and artifact collection complete"
        );
    
        Ok(BuildResponse::BuildComplete {
            unit_name: unit.package_name,
            artifacts,
        })
    }

    async fn start_heartbeat(&self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(100);
        
        // Periodic node cleanup
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

        // Heartbeat sender
        tokio::spawn({
            let nodes = self.nodes.clone();
            let local_node = self.local_node.clone();
            let tx_clone = tx.clone();
            async move {
                loop {
                    let nodes = nodes.read().await;
                    
                    for (node, _) in nodes.iter() {
                        if node != &local_node {
                            match TcpStream::connect(format!("{}:{}", node.host, node.port)).await {
                                Ok(mut stream) => {
                                    let msg = BuildRequest::Heartbeat;
                                    if let Ok(data) = bincode::serialize(&msg) {
                                        let len = (data.len() as u32).to_be_bytes();
                                        let _ = stream.write_all(&len).await;
                                        let _ = stream.write_all(&data).await;
                                    }
                                }
                                Err(e) => {
                                    warn!(host = %node.host, port = %node.port, "Heartbeat failed: {}", e);
                                }
                            }
                        }
                    }

                    let _ = tx_clone.send(()).await;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

        Ok(())
    }

    async fn build_on_node(&self, node: &Node, unit: BuildUnit, release: bool, target: Option<String>) -> Result<BuildResponse> {
        let mut stream = TcpStream::connect(format!("{}:{}", node.host, node.port)).await?;
        stream.set_nodelay(true)?;
        
        let tarball_data = Self::create_tarball(&unit)?;
        let request = BuildRequest::BuildUnit { 
            unit, 
            release, 
            target,
            tarball_data 
        };
        let data = bincode::serialize(&request)?;
        
        let len = (data.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&data).await?;
        
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await;
        let len = u32::from_be_bytes(len_buf);
        
        let mut buf = vec![0; len as usize];
        stream.read_exact(&mut buf).await?;
        
        Ok(bincode::deserialize(&buf)?)
    }

    async fn join_cluster(&self, seed: &str) -> Result<()> {
        let mut stream = TcpStream::connect(seed).await?;
        stream.set_nodelay(true)?;
        
        // Send heartbeat to seed node
        let msg = BuildRequest::Heartbeat;
        let data = bincode::serialize(&msg)?;
        let len = (data.len() as u32).to_be_bytes();
        stream.write_all(&len).await?;
        stream.write_all(&data).await?;

        // Read response
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf);
        
        let mut buf = vec![0; len as usize];
        stream.read_exact(&mut buf).await?;
        
        // Process seed node response
        match bincode::deserialize(&buf)? {
            BuildResponse::HeartbeatAck => {
                let mut nodes = self.nodes.write().await;
                nodes.insert(Node {
                    host: seed.split(':').next().unwrap().to_string(),
                    port: seed.split(':').nth(1).unwrap().parse()?,
                    cores: 0,
                }, time::Instant::now());
                Ok(())
            },
            _ => Err(anyhow::anyhow!("Invalid response from seed node"))
        }
    }

    async fn distribute_build(&self, workspace_path: PathBuf, release: bool, target: Option<String>) -> Result<()> {
        // Retrieve project metadata
        let metadata = MetadataCommand::new()
            .current_dir(&workspace_path)
            .exec()?;

        // Create dependency graph
        let mut graph = Graph::<BuildUnit, (), Directed>::new();
        let mut pkg_to_node = HashMap::new();

        // Build graph of build units
        for package in &metadata.packages {
            let source_files: Vec<PathBuf> = package.targets.iter()
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
                    .map(|t: &cargo_metadata::Target| PathBuf::from(&t.name))
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

        // Determine build order
        let build_order = petgraph::algo::toposort(&graph, None)
            .map_err(|_| anyhow::anyhow!("Cyclic dependencies detected"))?;

        // Prepare build futures
        let mut futures = Vec::new();
        let nodes = self.nodes.read().await;
        let mut node_iter = nodes.keys().cycle();

        // Schedule builds across nodes
        for unit_idx in build_order {
            let unit = graph[unit_idx].clone();
            if let Some(node) = node_iter.next() {
                let node = node.clone();
                let self_ref = self.clone();
                let target = target.clone();
                let build_future = async move {
                    self_ref.build_on_node(&node, unit, release, target).await
                };
                futures.push(build_future);
            }
        }

        // Execute builds and process results
        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(BuildResponse::BuildComplete { unit_name, artifacts }) => {
                    info!(unit = %unit_name, "Build complete");
                    let mut cache = self.build_cache.write().await;
                    for (path, data) in artifacts {
                        cache.insert(format!("{}:{}", unit_name, path.display()), data);
                    }
                }
                Ok(BuildResponse::BuildError { unit_name, error }) => {
                    error!(unit = %unit_name, "Build failed: {}", error);
                    return Err(anyhow::anyhow!("Build failed for {}: {}", unit_name, error));
                }
                _ => {}
            }
        }

        Ok(())
    }
}

async fn handle_connection(mut socket: TcpStream, cluster: Arc<BuildCluster>) -> Result<()> {
    socket.set_nodelay(true)?;
    
    loop {
        // Read request length
        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        
        // Read request data
        let mut buf = vec![0; len];
        socket.read_exact(&mut buf).await?;
        
        // Deserialize and process request
        let request: BuildRequest = match bincode::deserialize(&buf) {
            Ok(req) => req,
            Err(e) => {
                error!("Deserialization error: {}", e);
                continue;
            }
        };

        let response = match request {
            BuildRequest::BuildUnit { unit, release, target, tarball_data } => {
                match cluster.build_locally_with_tarball(unit, release, target, tarball_data).await {
                    Ok(resp) => resp,
                    Err(e) => BuildResponse::BuildError {
                        unit_name: "unknown".to_string(),
                        error: e.to_string(),
                    }
                }
            },
            BuildRequest::TransferArtifact { from_unit, artifact_path } => {
                let cache = cluster.build_cache.read().await;
                match cache.get(&format!("{}:{}", from_unit, artifact_path.display())) {
                    Some(data) => BuildResponse::BuildComplete {
                        unit_name: from_unit,
                        artifacts: vec![(artifact_path, data.clone())]
                    },
                    None => BuildResponse::BuildError {
                        unit_name: from_unit,
                        error: "Artifact not found".to_string()
                    }
                }
            },
            BuildRequest::Heartbeat => BuildResponse::HeartbeatAck
        };

        // Send response
        let response_data = bincode::serialize(&response)?;
        let len = (response_data.len() as u32).to_be_bytes();
        socket.write_all(&len).await?;
        socket.write_all(&response_data).await?;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let args = CliArgs::parse();

    // Setup logging
    let log_level = if args.debug { Level::DEBUG } else { Level::INFO };
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_max_level(log_level)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Create build configuration
    let config = Arc::new(BuildConfig {
        port: args.port,
        seed: args.seed,
        max_jobs: args.max_jobs,
        timeout: Duration::from_secs(args.timeout),
    });

    // Prepare build directory
    let build_dir = std::env::current_dir()?.join("builds");
    tokio::fs::create_dir_all(&build_dir).await?;

    // Initialize cluster
    let cluster = Arc::new(BuildCluster::new(config.clone(), build_dir));

    // Join cluster if seed node provided
    if let Some(seed) = config.seed.clone() {
        info!(seed = %seed, "Attempting to join cluster");
        match cluster.join_cluster(&seed).await {
            Ok(_) => info!("Successfully joined cluster"),
            Err(e) => warn!(error = %e, "Failed to join cluster"),
        }
    }

    // Start heartbeat mechanism
    cluster.start_heartbeat().await?;

    // Setup TCP listener
    let listener = TcpListener::bind(format!("0.0.0.0:{}", config.port)).await?;
    info!(port = %config.port, "Build node listening");

    // Connection handling
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
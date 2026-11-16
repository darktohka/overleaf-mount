use clap::Parser;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

pub mod client;
pub mod fs;
pub mod fs_state;
pub mod ot;
pub mod protocol;

use client::OverleafClient;
use fs::OverleafFs;
use fs_state::FsState;

static CHILD_PID: AtomicU32 = AtomicU32::new(0);

extern "C" fn handle_parent_signal(sig: libc::c_int) {
    let pid = CHILD_PID.load(Ordering::SeqCst);
    if pid > 0 {
        unsafe {
            libc::kill(pid as i32, sig);
        }
    }
}

pub fn is_mounted(mount_point: &Path) -> bool {
    let p_str = mount_point.to_string_lossy();
    let norm_path = if let Ok(canon) = mount_point.canonicalize() {
        canon.to_string_lossy().into_owned()
    } else {
        p_str.to_string()
    };

    if let Ok(content) = std::fs::read_to_string("/proc/mounts") {
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let mnt = parts[1];
                if mnt == p_str || mnt == norm_path {
                    return true;
                }
            }
        }
    }
    false
}

pub fn ensure_unmounted(mount_point: &Path) {
    if !is_mounted(mount_point) {
        return;
    }
    let c_path = match std::ffi::CString::new(mount_point.to_string_lossy().as_bytes()) {
        Ok(c) => c,
        Err(_) => return,
    };
    #[cfg(target_os = "linux")]
    {
        let _ = nix::mount::umount2(c_path.as_c_str(), nix::mount::MntFlags::MNT_DETACH);
    }
    for bin in &[
        "fusermount3",
        "/run/wrappers/bin/fusermount3",
        "/bin/fusermount3",
        "/usr/bin/fusermount3",
    ] {
        if let Ok(st) = std::process::Command::new(bin)
            .args(["-u", "-z", "-q", "--"])
            .arg(mount_point)
            .status()
        {
            if st.success() {
                break;
            }
        }
    }
    let _ = std::process::Command::new("umount")
        .args(["-l", "-q", "--"])
        .arg(mount_point)
        .status();
}

#[derive(Parser, Debug)]
#[command(author, version, about = "FUSE filesystem for Overleaf with real-time bidirectional sync")]
pub struct Cli {
    #[arg(short, long, default_value = "http://localhost:8088")]
    pub url: String,

    #[arg(short, long, default_value = "6afacb5e0082f5af0cb6d6f7")]
    pub project_id: String,

    #[arg(short, long, default_value = "admin@example.com")]
    pub email: Option<String>,

    #[arg(long, default_value = "Password123!")]
    pub password: Option<String>,

    #[arg(long)]
    pub cookie: Option<String>,

    #[arg(short, long)]
    pub mount_point: PathBuf,

    #[arg(long, default_value_t = false)]
    pub no_fast_sync: bool,

    #[arg(long, default_value_t = false)]
    pub debug: bool,

    #[arg(long, default_value_t = true, help = "Allow other users (including non-root) to access the mount")]
    pub allow_other: bool,

    #[arg(long, env = "OVERLEAF_UID", help = "User ID to own all files and folders in the mount")]
    pub uid: Option<u32>,

    #[arg(long, env = "OVERLEAF_GID", help = "Group ID to own all files and folders in the mount")]
    pub gid: Option<u32>,

    #[arg(long)]
    pub exec: Option<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // If running unprivileged, transparently re-exec in user namespace with CAP_SYS_ADMIN for FUSE
    if std::env::var("OVERLEAF_UNSHARED").is_err() && unsafe { libc::geteuid() != 0 } {
        if let Ok(exe) = std::env::current_exe() {
            let orig_uid = unsafe { libc::getuid() };
            let orig_gid = unsafe { libc::getgid() };

            // Parse --uid, --gid, and --mount-point from arguments or env if present
            let mut target_uid = orig_uid;
            let mut target_gid = orig_gid;
            let mut mount_point: Option<PathBuf> = None;
            let args: Vec<String> = std::env::args().collect();
            let mut i = 1;
            while i < args.len() {
                if (args[i] == "--uid" || args[i] == "-u") && i + 1 < args.len() {
                    if let Ok(u) = args[i + 1].parse::<u32>() {
                        target_uid = u;
                    }
                    i += 2;
                    continue;
                } else if args[i].starts_with("--uid=") {
                    if let Ok(u) = args[i]["--uid=".len()..].parse::<u32>() {
                        target_uid = u;
                    }
                } else if (args[i] == "--gid" || args[i] == "-g") && i + 1 < args.len() {
                    if let Ok(g) = args[i + 1].parse::<u32>() {
                        target_gid = g;
                    }
                    i += 2;
                    continue;
                } else if args[i].starts_with("--gid=") {
                    if let Ok(g) = args[i]["--gid=".len()..].parse::<u32>() {
                        target_gid = g;
                    }
                } else if (args[i] == "--mount-point" || args[i] == "-m") && i + 1 < args.len() {
                    mount_point = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                    continue;
                } else if args[i].starts_with("--mount-point=") {
                    mount_point = Some(PathBuf::from(&args[i]["--mount-point=".len()..]));
                }
                i += 1;
            }

            if let Ok(u_str) = std::env::var("OVERLEAF_UID") {
                if let Ok(u) = u_str.parse::<u32>() {
                    target_uid = u;
                }
            }
            if let Ok(g_str) = std::env::var("OVERLEAF_GID") {
                if let Ok(g) = g_str.parse::<u32>() {
                    target_gid = g;
                }
            }
            if let Ok(m_str) = std::env::var("OVERLEAF_MOUNT_POINT") {
                mount_point = Some(PathBuf::from(m_str));
            }

            let mut child = std::process::Command::new("unshare")
                .args([
                    "-m",
                    "-U",
                    "--keep-caps",
                    &format!("--map-user={}", target_uid),
                    &format!("--map-group={}", target_gid),
                ])
                .env("OVERLEAF_UNSHARED", "1")
                .env("OVERLEAF_ORIG_UID", orig_uid.to_string())
                .env("OVERLEAF_ORIG_GID", orig_gid.to_string())
                .arg(exe)
                .args(std::env::args().skip(1))
                .spawn()?;

            let child_id = child.id();
            CHILD_PID.store(child_id, Ordering::SeqCst);

            unsafe {
                libc::signal(libc::SIGINT, handle_parent_signal as *const () as libc::sighandler_t);
                libc::signal(libc::SIGTERM, handle_parent_signal as *const () as libc::sighandler_t);
                libc::signal(libc::SIGHUP, handle_parent_signal as *const () as libc::sighandler_t);
            }

            let status = child.wait();

            if let Some(mp) = &mount_point {
                ensure_unmounted(mp);
            }

            if let Ok(st) = status {
                if let Some(code) = st.code() {
                    std::process::exit(code);
                }
            }
            return Ok(());
        }
    }

    // Reset signal handlers to default inside child / root process so Tokio handles them cleanly
    unsafe {
        libc::signal(libc::SIGINT, libc::SIG_DFL);
        libc::signal(libc::SIGTERM, libc::SIG_DFL);
        libc::signal(libc::SIGHUP, libc::SIG_DFL);
    }

    let cli = Cli::parse();

    if cli.debug {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
    }

    if !cli.mount_point.exists() {
        std::fs::create_dir_all(&cli.mount_point)?;
    }

    // Register panic hook to guarantee unmount on panic
    let mp_panic = cli.mount_point.clone();
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        ensure_unmounted(&mp_panic);
        default_panic(info);
    }));

    let fs_state = FsState::new(cli.uid, cli.gid);

    // Start Tokio runtime for async WebSocket and HTTP synchronization
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let (client, command_rx) = rt.block_on(async {
        OverleafClient::new(
            cli.url.clone(),
            cli.project_id.clone(),
            cli.email.clone(),
            cli.password.clone(),
            cli.cookie.clone(),
            fs_state.clone(),
            cli.no_fast_sync,
        )
        .await
    })?;

    // Spawn sync loop in Tokio background
    let client_sync = client.clone();
    rt.spawn(async move {
        client_sync.run_sync_loop(command_rx).await;
    });

    // Allow 1 second for initial handshake & project tree population
    std::thread::sleep(Duration::from_millis(1000));

    let fs = OverleafFs::new(fs_state, client);

    let mut config = fuser::Config::default();
    config.mount_options.push(fuser::MountOption::FSName("overleaf-fs".to_string()));
    config.mount_options.push(fuser::MountOption::DefaultPermissions);
    config.acl = if cli.allow_other {
        fuser::SessionACL::All
    } else {
        fuser::SessionACL::Owner
    };
    config.n_threads = Some(4);
    config.clone_fd = false;

    tracing::info!("Mounting Overleaf project {} at {:?}", cli.project_id, cli.mount_point);

    let session = fuser::spawn_mount(fs, &cli.mount_point, &config)?;
    tracing::info!("Filesystem mounted successfully at {:?}", cli.mount_point);

    if let Some(cmd) = cli.exec {
        let exit_code = rt.block_on(async {
            let mut child = tokio::process::Command::new("sh")
                .args(["-c", &cmd])
                .spawn()?;

            let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

            let code = tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(st) => st.code().unwrap_or(0),
                        Err(_) => 1,
                    }
                }
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT (Ctrl+C), stopping exec process and unmounting...");
                    let _ = child.kill().await;
                    130
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, stopping exec process and unmounting...");
                    let _ = child.kill().await;
                    143
                }
                _ = sighup.recv() => {
                    tracing::info!("Received SIGHUP, stopping exec process and unmounting...");
                    let _ = child.kill().await;
                    129
                }
            };
            Ok::<i32, std::io::Error>(code)
        })?;

        tracing::info!("Unmounting filesystem at {:?}...", cli.mount_point);
        let _ = session.umount_and_join();
        ensure_unmounted(&cli.mount_point);

        if exit_code != 0 {
            std::process::exit(exit_code);
        }
    } else {
        tracing::info!("Filesystem ready. Press Ctrl+C or send SIGTERM to unmount and exit.");
        rt.block_on(async {
            let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            let mut sighup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;

            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT (Ctrl+C), unmounting filesystem at {:?}...", cli.mount_point);
                }
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, unmounting filesystem at {:?}...", cli.mount_point);
                }
                _ = sighup.recv() => {
                    tracing::info!("Received SIGHUP, unmounting filesystem at {:?}...", cli.mount_point);
                }
                _ = async {
                    loop {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                        if session.guard.is_finished() {
                            break;
                        }
                    }
                } => {
                    tracing::info!("Filesystem session ended (unmounted externally).");
                }
            }
            Ok::<(), std::io::Error>(())
        })?;

        tracing::info!("Unmounting filesystem at {:?}...", cli.mount_point);
        let _ = session.umount_and_join();
        ensure_unmounted(&cli.mount_point);
        tracing::info!("Filesystem cleanly unmounted.");
    }

    Ok(())
}

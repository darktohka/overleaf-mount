//! `overleaf-mount` — mount an Overleaf project as a local filesystem.
//!
//! Every write is pushed to Overleaf within milliseconds (over the real-time
//! WebSocket when available, HTTP fallback otherwise) and every edit made by
//! collaborators shows up locally.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use log::{error, info, warn};

use overleaf_mount::fuse::{FuseNotifier, OverleafFs, TTL};
use overleaf_mount::gitignore::Ignorer;
use overleaf_mount::overleaf::OverleafClient;
use overleaf_mount::sync::SyncEngine;
use overleaf_mount::types::{Fs, ROOT_INO};

#[derive(Parser, Debug)]
#[command(
    name = "overleaf-mount",
    about = "Mount an Overleaf project as a FUSE filesystem with live OT sync",
    version
)]
struct Args {
    /// Overleaf server base URL, e.g. https://www.overleaf.com
    #[arg(long, env = "OVERLEAF_URL", default_value = "http://localhost:8080")]
    url: String,

    /// Project id (the 24-hex id in the project URL)
    #[arg(long, env = "OVERLEAF_PROJECT")]
    project: String,

    /// Project-scoped token (starts with `olp_`). Alternative to --email/--password.
    #[arg(long, env = "OVERLEAF_TOKEN")]
    token: Option<String>,

    /// Account email for a session login (dev servers, CE instances)
    #[arg(long, env = "OVERLEAF_EMAIL")]
    email: Option<String>,

    /// Account password for a session login
    #[arg(long, env = "OVERLEAF_PASSWORD")]
    password: Option<String>,

    /// Mount point
    mount: PathBuf,

    /// Owner uid for every file/dir (default: the calling user's uid)
    #[arg(long)]
    uid: Option<u32>,

    /// Owner gid for every file/dir
    #[arg(long)]
    gid: Option<u32>,

    /// Allow other users to access the mount
    #[arg(long, default_value_t = false)]
    allow_other: bool,

    /// Run in the foreground (do not daemonise)
    #[arg(short, long, default_value_t = true)]
    foreground: bool,

    /// Do not unmount on exit (for debugging)
    #[arg(long, default_value_t = false)]
    no_auto_unmount: bool,

    /// Extra .gitignore file to honour (in addition to the project's own)
    #[arg(long)]
    gitignore: Option<PathBuf>,

    /// Base URL of the real-time service for socket.io. Defaults to the main
    /// host (nginx proxies /socket.io there); point it at e.g.
    /// http://127.0.0.1:3026 when running a bare dev stack.
    #[arg(long, env = "OVERLEAF_REALTIME_URL")]
    realtime_url: Option<String>,

    /// Username for Overleaf's private API (enables document read/write over
    /// HTTP as a fallback when the socket is unavailable)
    #[arg(long, env = "OVERLEAF_API_USER")]
    api_user: Option<String>,

    /// Password for Overleaf's private API
    #[arg(long, env = "OVERLEAF_API_PASSWORD")]
    api_password: Option<String>,
}

fn now_uid() -> u32 {
    unsafe { libc::getuid() }
}

fn now_gid() -> u32 {
    unsafe { libc::getgid() }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("overleaf_mount=info,warn"),
    )
    .format_timestamp_millis()
    .init();

    let args = Args::parse();
    let uid = args.uid.unwrap_or_else(now_uid);
    let gid = args.gid.unwrap_or_else(now_gid);

    if !args.mount.exists() {
        std::fs::create_dir_all(&args.mount)
            .with_context(|| format!("creating mount point {}", args.mount.display()))?;
    }

    info!(
        "connecting to {} project {}",
        args.url, args.project
    );
    let client = OverleafClient::connect(
        &args.url,
        &args.project,
        args.token.as_deref(),
        match (&args.email, &args.password) {
            (Some(e), Some(p)) => Some((e.as_str(), p.as_str())),
            _ => None,
        },
    )
    .await
    .context("authenticating with Overleaf")?
    .with_realtime(args.realtime_url.as_deref())
    .with_api_auth(args.api_user.as_deref(), args.api_password.as_deref());

    let fs = Arc::new(Fs::new(uid, gid, TTL));
    *fs.mount_point.write() = args.mount.display().to_string();

    let mount_generation = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let notifier = Arc::new(FuseNotifier {
        fs: fs.clone(),
        mount_generation: mount_generation.clone(),
    });
    let (engine, handle) = SyncEngine::new(fs.clone(), client.clone(), notifier);

    // Initial load before the mount becomes visible, so `ls` is immediately
    // correct and reads do not block on the network.
    engine.bootstrap().await.context("loading project")?;

    // .gitignore: read the project's own file (if the project has one) and any
    // user-supplied extra rules. Matching files stay local-only.
    //
    // `.git` is always local-only: the mount is a working copy, and pushing
    // version-control internals into the Overleaf project (as thousands of
    // docs and folders) is never what the user wants. The anchored rule also
    // covers everything below it.
    let mut rules = String::from("/.git\n");
    if let Some(extra) = &args.gitignore {
        if let Ok(text) = std::fs::read_to_string(extra) {
            rules.push_str(&text);
        }
    } else if let Some(ino) = fs.lookup(ROOT_INO, ".gitignore") {
        if let Some(doc) = fs.doc(ino) {
            rules.push_str(&doc.text());
            info!("honouring the project's .gitignore");
        }
    }
    let ignorer = Ignorer::with_root_rules(&rules);
    // Record which tree entries are ignored, and drop them from the sync set.
    {
        let paths: Vec<(u64, String)> = fs
            .nodes
            .read()
            .values()
            .map(|n| (n.ino, fs.path_of(n.ino)))
            .collect();
        for (ino, path) in paths {
            if !path.is_empty() && ignorer.is_ignored(&path) {
                fs.mark_ignored(&path, true);
                if let Some(n) = fs.nodes.write().get_mut(&ino) {
                    // keep the node visible but as a local-only file
                    if matches!(n.kind, overleaf_mount::types::NodeKind::Doc { .. }) {
                        n.kind = overleaf_mount::types::NodeKind::LocalFile;
                    }
                }
                debug_ignored(&path);
            }
        }
    fs.set_ignorer(ignorer);
    }

    let engine_task = {
        let engine = engine.clone();
        tokio::spawn(async move {
            if let Err(e) = engine.run().await {
                warn!("sync engine finished: {e:#}");
            }
        })
    };

    // Mount in a blocking thread; FUSE's session loop is synchronous.
    let fuse_fs = OverleafFs::new(
        fs.clone(),
        handle.clone(),
        client.clone(),
        tokio::runtime::Handle::current(),
    );
    let mountpoint = args.mount.clone();
    let allow_other = args.allow_other;
    let auto_unmount = !args.no_auto_unmount;
    let shutdown = Arc::new(AtomicBool::new(false));

    install_signal_handlers(fs.clone(), mountpoint.clone(), shutdown.clone())?;

    info!("mounting at {}", mountpoint.display());
    let mut options = vec![
        fuser::MountOption::FSName("overleaf".to_string()),
        fuser::MountOption::Subtype("overleaf".to_string()),
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::RW,
    ];
    if auto_unmount {
        options.push(fuser::MountOption::AutoUnmount);
    }
    // fuser mounts auto_unmount filesystems with allow_other (fusermount
    // requires it) but still decides the *userspace* ACL from the options the
    // caller passed: without an explicit AllowOther it uses SessionACL::Owner
    // and rejects every request whose uid is not the daemon's. That makes a
    // root-started mount with `--uid 1000` unusable for uid 1000 — lookup,
    // getattr and open all come back EACCES even though the kernel would allow
    // them. Ask for allow_other ourselves so fuser accepts the requests and the
    // kernel's default_permissions does the actual checking.
    if allow_other || auto_unmount {
        options.push(fuser::MountOption::AllowOther);
    }

    let mp = mountpoint.clone();
    let mount_result = tokio::task::spawn_blocking(move || {
        fuser::mount2(fuse_fs, &mp, &options).map_err(|e| anyhow!("fuse mount failed: {e}"))
    })
    .await
    .context("joining mount task")?;

    // Unmounted cleanly: stop the sync engine.
    engine_task.abort();
    match mount_result {
        Ok(()) => info!("unmounted {}", mountpoint.display()),
        Err(e) => {
            error!("{e:#}");
            return Err(e);
        }
    }
    Ok(())
}

fn debug_ignored(path: &str) {
    info!("local-only (gitignored): {path}");
}

/// Ctrl+C / SIGTERM / SIGHUP unmount the filesystem and exit.
fn install_signal_handlers(
    fs: Arc<Fs>,
    mountpoint: PathBuf,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    // Deliberately NOT SIGHUP: a mount backgrounded with nohup/`&` must survive
    // the shell that started it going away, and installing a handler would
    // override nohup's SIG_IGN.
    for sig in [
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGQUIT,
    ] {
        signal_hook::flag::register(sig, shutdown.clone())?;
    }
    // One watcher for the shared flag: spawning a thread per signal made all of
    // them race fusermount3 for the same mountpoint, so one unmount succeeded
    // while the others printed "failed to unmount …: Invalid argument" (and
    // "not mounted" from the umount fallback).
    std::thread::Builder::new()
        .name("signal-unmount".to_string())
        .spawn(move || {
            // Wait for the flag to be set by any registered signal.
            while !shutdown.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(50));
            }
            info!("signal received: unmounting {}", mountpoint.display());
            for _ in 0..40 {
                let status = std::process::Command::new("fusermount3")
                    .arg("-u")
                    .arg("-z")
                    .arg(&mountpoint)
                    .status();
                match status {
                    Ok(s) if s.success() => break,
                    _ => {
                        let alt = std::process::Command::new("umount")
                            .arg("-l")
                            .arg(&mountpoint)
                            .status();
                        if matches!(alt, Ok(s) if s.success()) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
            let _ = fs.bump();
            std::process::exit(0);
        })
        .ok();
    Ok(())
}

/// Unused helper kept for symmetry with the client API.
#[allow(dead_code)]
async fn health(client: &OverleafClient) -> Result<()> {
    if !overleaf_mount::overleaf::try_status(client).await {
        bail!("server is not healthy");
    }
    Ok(())
}

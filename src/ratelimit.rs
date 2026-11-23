//! Global rate-limit awareness for the Overleaf REST API.
//!
//! Overleaf rate-limits per *user* on the web routes this mount uses, with
//! different budgets per endpoint (see `EditorRouter`/`UploadsRouter`):
//!
//! | endpoint                         | budget        |
//! |----------------------------------|---------------|
//! | `POST /project/:id/doc`          | 30 / minute   |
//! | `POST /project/:id/folder`       | 60 / minute   |
//! | `POST /Project/:id/upload`       | 500 / 15 min  |
//! | `POST /project/:id/join`         | 45 / minute   |
//!
//! Those limits are per client, so *every* request the mount sends competes for
//! the same budget — document pushes, folder creation, uploads, polls and the
//! login handshake. A single limiter shared by all of them is therefore the only
//! place where pacing can be correct.
//!
//! Two mechanisms:
//!
//! * **Proactive pacing.** Each endpoint class keeps a sliding window of the
//!   instants its requests were *started*. A request may start only when the
//!   window has a free slot, so a burst (a repository with 30 object fan-out
//!   folders) is spread over the server's window instead of bouncing off a 429.
//! * **Reactive cooldown.** The limiter also holds one *global* cooldown. A 429
//!   on any endpoint sets it (honouring `Retry-After` when the server sends it,
//!   escalating the wait across consecutive rejections), and every request of
//!   every class waits for it. That is what stops 20 pumps from each retrying
//!   into an already-closed limiter window.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use parking_lot::Mutex;

/// An endpoint class with its own server-side budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Endpoint {
    AddDoc,
    AddFolder,
    Upload,
    JoinProject,
    /// Everything else (polls, reads, renames, deletes, login): no known
    /// budget of its own, paced only by the global cooldown.
    Other,
}

impl Endpoint {
    pub fn class(self) -> &'static str {
        match self {
            Endpoint::AddDoc => "add-doc",
            Endpoint::AddFolder => "add-folder",
            Endpoint::Upload => "upload",
            Endpoint::JoinProject => "join-project",
            Endpoint::Other => "api",
        }
    }

    /// `(requests, window)` as enforced by the Overleaf web routes.
    fn budget(self) -> Option<(usize, Duration)> {
        match self {
            Endpoint::AddDoc => Some((30, Duration::from_secs(60))),
            Endpoint::AddFolder => Some((60, Duration::from_secs(60))),
            Endpoint::Upload => Some((500, Duration::from_secs(15 * 60))),
            Endpoint::JoinProject => Some((45, Duration::from_secs(60))),
            Endpoint::Other => None,
        }
    }

    /// Classify a request by method and URL path. Doing this centrally (rather
    /// than at ~15 call sites) keeps the budgets attached to the routes that
    /// actually have them, including paths added later.
    pub fn classify(method: &str, path: &str) -> Endpoint {
        let path = path.trim_end_matches('/');
        let post = method.eq_ignore_ascii_case("post");
        if post && path.ends_with("/upload") {
            Endpoint::Upload
        } else if post && path.ends_with("/folder") {
            Endpoint::AddFolder
        } else if post && path.ends_with("/doc") {
            // `/project/:id/doc` (create) vs `/project/:id/doc/:doc_id` (write).
            Endpoint::AddDoc
        } else if post && path.ends_with("/join") {
            Endpoint::JoinProject
        } else {
            Endpoint::Other
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.class())
    }
}

/// A 429 from Overleaf, preserved as a typed error so callers can react to the
/// limit itself instead of pattern-matching on a formatted message.
#[derive(Debug)]
pub struct RateLimited {
    /// What was being attempted, e.g. `add_folder util`.
    pub action: String,
    /// The endpoint class that was rejected.
    pub class: &'static str,
    /// `Retry-After`, when the server sent one.
    pub retry_after: Option<Duration>,
}

impl RateLimited {
    pub fn new(action: impl Into<String>, class: Endpoint, retry_after: Option<Duration>) -> Self {
        Self {
            action: action.into(),
            class: class.class(),
            retry_after,
        }
    }
}

impl fmt::Display for RateLimited {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} -> 429 Too Many Requests ({} rate limit)",
            self.action, self.class
        )?;
        if let Some(d) = self.retry_after {
            write!(f, ", retry after {:.1}s", d.as_secs_f32())?;
        }
        Ok(())
    }
}

impl std::error::Error for RateLimited {}

#[derive(Debug, Default)]
struct Cooldown {
    until: Option<Instant>,
}

/// The shared limiter. Cheap to clone by reference; one instance per client.
#[derive(Debug, Default)]
pub struct RateLimiter {
    /// Start instants of requests inside the current window, per class.
    windows: Mutex<HashMap<Endpoint, VecDeque<Instant>>>,
    /// Nothing may be sent before this instant.
    cooldown: Mutex<Cooldown>,
    /// Consecutive 429s, used to escalate the cooldown.
    rejections: AtomicU32,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait until this request may be sent, then reserve its slot.
    ///
    /// Awaiting here (rather than at the call sites) is what makes the limit
    /// global: every code path — pumps, tree refreshes, polls, uploads, the
    /// login handshake — goes through it.
    pub async fn acquire(&self, endpoint: Endpoint) {
        loop {
            self.wait_for_cooldown().await;
            let now = Instant::now();
            let wait = {
                let mut windows = self.windows.lock();
                match endpoint.budget() {
                    None => None,
                    Some((points, duration)) => {
                        let window = windows.entry(endpoint).or_default();
                        while window
                            .front()
                            .is_some_and(|t| now.duration_since(*t) >= duration)
                        {
                            window.pop_front();
                        }
                        if window.len() < points {
                            window.push_back(now);
                            None
                        } else {
                            // The oldest request still inside the window frees
                            // its slot when `duration` has elapsed for it.
                            window
                                .front()
                                .map(|t| (*t + duration).saturating_duration_since(now))
                        }
                    }
                }
            };
            match wait {
                // 50 ms of slack: the server's clock and ours differ slightly,
                // and a request landing a millisecond early is still a 429.
                Some(d) => tokio::time::sleep(d + Duration::from_millis(50)).await,
                None => return,
            }
        }
    }

    /// Block while the server-wide cooldown is in effect.
    pub async fn wait_for_cooldown(&self) {
        loop {
            let remaining = {
                let guard = self.cooldown.lock();
                guard
                    .until
                    .and_then(|until| until.checked_duration_since(Instant::now()))
            };
            match remaining {
                Some(d) if !d.is_zero() => tokio::time::sleep(d).await,
                _ => return,
            }
        }
    }

    /// True while a 429 cooldown is in effect. Poll loops use this to skip a
    /// request altogether instead of queueing behind the cooldown.
    pub fn is_cooling_down(&self) -> bool {
        self.cooldown
            .lock()
            .until
            .is_some_and(|until| until > Instant::now())
    }

    /// Time left on the cooldown, for logging.
    pub fn cooldown_remaining(&self) -> Option<Duration> {
        self.cooldown
            .lock()
            .until
            .and_then(|until| until.checked_duration_since(Instant::now()))
    }

    /// Record a successful request; clears the escalation counter once the
    /// cooldown has lapsed (a success *during* a cooldown cannot happen, since
    /// every request waits for it, so this is effectively "we are healthy").
    pub fn note_success(&self) {
        if !self.is_cooling_down() {
            self.rejections.store(0, Ordering::Relaxed);
        }
    }

    /// Record a 429 and extend the shared cooldown. Returns the applied delay.
    ///
    /// The wait escalates with consecutive rejections (3 s, 6 s, 12 s, 24 s,
    /// capped at 45 s) because Overleaf's limiter is a fixed window: the first
    /// 429 can arrive just before the window resets, but a second one means we
    /// are still inside it and retrying at the old cadence would keep the window
    /// full. `Retry-After` wins when it is longer.
    pub fn note_rate_limited(&self, retry_after: Option<Duration>) -> Duration {
        let streak = self.rejections.fetch_add(1, Ordering::Relaxed).min(5);
        let escalated = Duration::from_secs(3 * 2u64.pow(streak)).min(Duration::from_secs(45));
        let delay = retry_after.unwrap_or_default().max(escalated);
        let until = Instant::now() + delay;
        let mut guard = self.cooldown.lock();
        if guard.until.map_or(true, |current| current < until) {
            guard.until = Some(until);
        }
        delay
    }

    /// Requests currently counted in a class window (test/diagnostic helper).
    pub fn window_len(&self, endpoint: Endpoint) -> usize {
        self.windows
            .lock()
            .get(&endpoint)
            .map(VecDeque::len)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_the_limited_routes() {
        assert_eq!(Endpoint::classify("POST", "/project/abc/doc"), Endpoint::AddDoc);
        assert_eq!(Endpoint::classify("POST", "/project/abc/doc/"), Endpoint::AddDoc);
        assert_eq!(
            Endpoint::classify("POST", "/project/abc/doc/6abe0f"),
            Endpoint::Other
        );
        assert_eq!(
            Endpoint::classify("POST", "/project/abc/folder"),
            Endpoint::AddFolder
        );
        assert_eq!(
            Endpoint::classify("POST", "/Project/abc/upload"),
            Endpoint::Upload
        );
        assert_eq!(
            Endpoint::classify("POST", "/project/abc/join"),
            Endpoint::JoinProject
        );
        assert_eq!(
            Endpoint::classify("GET", "/project/abc/updates"),
            Endpoint::Other
        );
    }

    #[tokio::test(start_paused = true)]
    async fn paces_a_class_to_its_budget() {
        let limiter = RateLimiter::new();
        // 30 doc creations may start immediately...
        for _ in 0..30 {
            limiter.acquire(Endpoint::AddDoc).await;
        }
        assert_eq!(limiter.window_len(Endpoint::AddDoc), 30);
        // ...and the 31st waits for the window to slide.
        let start = Instant::now();
        limiter.acquire(Endpoint::AddDoc).await;
        assert!(start.elapsed() >= Duration::from_secs(59));
    }

    #[tokio::test(start_paused = true)]
    async fn a_429_cools_every_class_down() {
        let limiter = RateLimiter::new();
        let delay = limiter.note_rate_limited(None);
        assert!(delay >= Duration::from_secs(3));
        assert!(limiter.is_cooling_down());
        let start = Instant::now();
        // A *different* endpoint must wait too: the limit is per user.
        limiter.acquire(Endpoint::AddFolder).await;
        assert!(start.elapsed() >= delay);
        assert!(!limiter.is_cooling_down());
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_wins_when_longer() {
        let limiter = RateLimiter::new();
        let delay = limiter.note_rate_limited(Some(Duration::from_secs(90)));
        assert_eq!(delay, Duration::from_secs(90));
    }

    #[test]
    fn escalates_on_consecutive_rejections() {
        let limiter = RateLimiter::new();
        let first = limiter.note_rate_limited(None);
        let second = limiter.note_rate_limited(None);
        assert!(second > first);
        for _ in 0..10 {
            limiter.note_rate_limited(None);
        }
        assert_eq!(limiter.note_rate_limited(None), Duration::from_secs(45));
    }
}

# overleaf-mount

Mount an Overleaf project as a local filesystem. Every write to the mount is
pushed to Overleaf within milliseconds, and every edit made by a collaborator
shows up on the mount — including when both happen to the same sentence at the
same time.

```
┌──────────┐   write(2)   ┌───────────────┐  applyOtUpdate   ┌────────────────┐
│  editor  │ ───────────> │ overleaf-mount│ ───────────────> │  real-time     │
│  (vim,   │              │  FUSE + OT    │ <─────────────── │  document-     │
│  latexmk)│ <─────────── │    engine     │  otUpdateApplied │  updater       │
└──────────┘   read(2)    └───────────────┘                  └────────────────┘
```

## Quick start

```sh
# development server (session login + private API credentials)
overleaf-mount \
  --url http://127.0.0.1:3000 \
  --realtime-url http://127.0.0.1:3026 \
  --api-user sharelatex --api-password dev-api-password \
  --project <24-hex-project-id> \
  --email mount-test@example.com --password mount-test-password \
  --uid 1000 --gid 100 \
  ~/overleaf

# overleaf.com / a CE instance behind nginx (project token)
overleaf-mount --project <24-hex-project-id> --token olp_... ~/overleaf
```

Ctrl+C or `SIGTERM` unmounts cleanly, and the mount is created with
`auto_unmount`, so a killed process cannot leave a stale mountpoint behind.

## Options

| Flag | Meaning |
| --- | --- |
| `--url <URL>` | Server base URL (env `OVERLEAF_URL`), default `http://localhost:8080` |
| `--project <ID>` | 24-hex project id from the project URL (env `OVERLEAF_PROJECT`) |
| `--token <TOKEN>` | Project-scoped token (`olp_…`); alternative to email/password |
| `--email`, `--password` | Session login for dev servers and CE instances |
| `--realtime-url <URL>` | socket.io host; defaults to `--url` (nginx proxies `/socket.io` there). Point it at `http://127.0.0.1:3026` for a bare dev stack |
| `--api-user`, `--api-password` | Credentials for web's private API; enables project/document HTTP access and the no-socket fallback |
| `--uid`, `--gid` | Owner of every file and directory on the mount (default: the caller's) |
| `--allow-other` | Let other users access the mount |
| `--gitignore <FILE>` | Extra ignore file (default: the project's own `.gitignore`) |
| `--no-auto-unmount` | Debugging aid: leave the mount in place on exit |

`OVERLEAF_URL`, `OVERLEAF_PROJECT`, `OVERLEAF_TOKEN`, `OVERLEAF_EMAIL`,
`OVERLEAF_PASSWORD`, `OVERLEAF_REALTIME_URL`, `OVERLEAF_API_USER` and
`OVERLEAF_API_PASSWORD` work as environment variables too.

## Behaviour

* **Documents are text, not blobs.** Each Overleaf document is a file whose
  content is its line array joined with `\n`.
* **Ownership is settable.** `--uid`/`--gid` apply to everything, including files
  created later.
* **chmod / chown / utimes are in memory only.** Honoured by the kernel for the
  lifetime of the mount, never sent to Overleaf (which has no such concept for
  documents).
* **`.sh` files are `0755` by default** (everything else `0644`), so
  `chmod +x build.sh` is unnecessary.
* **`.gitignore` is honoured.** Matching paths are never created on the server;
  they live in the mount's memory, so `*.aux`, `*.log`, `build/` behave normally
  for `latexmk` without polluting the project. Nested `.gitignore` files and
  `!re-include` rules work, and an ancestor match (`build/`) covers children.
* **Binary uploads** are read-only (`EROFS` on write): Overleaf's OT types have no
  notion of a blob edit.

## How synchronisation works

### The live path

The mount speaks Overleaf's **socket.io 0.9** protocol (`5:::{…}`), the same
channel the browser editor uses:

* `GET /socket.io/1/?projectId=<id>` → `<session>:<heartbeat>:<close>:<transports>`
* `ws /socket.io/1/websocket/<session>?projectId=<id>`
* `joinProject` → `joinProjectResponse` carries the whole tree **with document
  ids** (the only authoritative source for them).
* `joinDoc` (with an ack id) → `[null, lines, version, ops, ranges, type,
  {canSkipLeaveDoc}]`.
* our edits go out as `applyOtUpdate` with
  `{doc, op: [{p,i}|{p,d}], v: <version the diff was computed against>}`;
* every applied edit arrives as `otUpdateApplied {doc, v, op?}`. **`v` is the
  version the op was applied *to*** (the document becomes `v+1`), and for *our
  own* edit the server omits `op` — that frame is the confirmation.

### Conflict resolution

Because our operation carries the version it was computed against,
document-updater transforms it against anything that arrived first. That covers
the interesting cases by construction:

* **Appends and distant edits** merge without interaction.
* **Two people editing the same sentence** both keep their text, ordered by the
  server; nothing is dropped and no conflict markers appear. Verified by
  `scenarios.mjs` (one client rewrites a sentence while another rewrites the
  same sentence from the same base version).
* **A collaborator's edit arriving while ours is in flight** is handled by
  tracking the in-flight op: the acknowledgement cannot describe the transformed
  result, so the document is re-read instead of guessed at.

Two bookkeeping rules make this work, and both were bugs once:

1. The **OT base is what the server confirmed**, never the visible file (the FUSE
   layer updates that optimistically) and never "the visible text at the echoed
   version" (which can already contain a newer remote edit).
2. A local change is expressed as `diff(from → desired)` and **rebased** onto
   the server's current text before it is sent (`rebase_local`). Pushing the raw
   local snapshot instead would silently delete a collaborator's concurrent edit.

Related kernel-level details that are easy to get wrong:

* `truncate(0)` and the following `write(2)` arrive as separate FUSE calls and
  can be delivered out of order. Each write intent carries a sequence number and
  stale ones are dropped; a short coalescing window merges the pair anyway.
* `st_mode` must include the file-type bits (`S_IFDIR`/`S_IFREG`), otherwise the
  root looks like a regular file and every `ls` fails with `EIO`.
* Writes are never buffered per handle: each one splices into the current
  document text, so a truncate or a remote edit cannot be reverted by a stale
  copy taken at `open(2)` time.

### Fallbacks

1. **No document ids / no socket at start-up:** the tree comes from web's
   private `POST /project/:id/join` (credentials from `--api-user/--api-password`,
   user id read from the `ol-user_id` meta tag), exactly what real-time calls.
   `/project/:id/entities` is the last resort and yields names only.
2. **Socket drops while running:** edits are kept in `desired` and retried with
   backoff; the socket reconnects every 2 s and the edit is delivered then (this
   is what the suite's outage scenario proves). While the socket is down,
   documents are re-read through the private document API, and a write is also
   attempted as a whole-document `POST /project/:id/doc/:id`.
3. **Every Overleaf deployment runs real-time**, so the socket path is the normal
   one; the fallbacks exist for degraded states and for servers without the
   optimisations described here. Nothing in this crate requires a patched
   Overleaf.

### No changes required in Overleaf

Nothing in this crate requires a patched Overleaf server: the socket path uses
the same protocol the stock editor uses, and the fallbacks use documented
service APIs. `patches/0003-feat-add-fast-sync-endpoint-for-direct-document-content-updates.patch`
in the repository adds an optional `POST /project/:id/doc/:id/fast-sync` endpoint
(whole-document upload in one request); it was **not** adopted, because the
incremental op path is already a single small frame per edit and works against
unmodified servers. It remains available if whole-document uploads ever need to
be cheaper than a diff.

`SIGHUP` is deliberately **not** intercepted: a mount started with `nohup … &`
must survive its shell going away, and installing a handler would override
`nohup`'s `SIG_IGN`. `SIGINT`, `SIGTERM` and `SIGQUIT` unmount cleanly.

## Architecture

| File | Responsibility |
| --- | --- |
| `src/ot.rs` | `TextOp` (retain / insert / remove), `diff`, `transform_pair`, `compose`, apply. Fuzzed for convergence over 2000 random concurrent edit pairs. |
| `src/merge3.rs` | Line-based diff3 (used only by the polling fallback). |
| `src/sync.rs` | socket.io 0.9 client, ack registry, OT base + in-flight op tracking, rebase, remote/own update handling, push pump, polling loop. |
| `src/overleaf.rs` | HTTP client: login + CSRF handshake, manual redirects (so the session cookie on a 302 survives), `/join` tree, private document API, cookie jar. |
| `src/types.rs` | In-memory image: nodes, attributes (`full_mode` carries the type bits), docs, ignore flags, chmod/chown state. |
| `src/fuse.rs` | FUSE callbacks. All of them touch memory only; writes hand a sequenced intent to the engine so a network round-trip never blocks a kernel thread. |
| `src/gitignore.rs` | Ancestor-aware matching over the `ignore` crate's matchers. |

## Tests

```sh
cd overleaf-mount && cargo test --lib     # 29 unit tests
```

Covers OT transform/compose invariants plus a 2000-round convergence fuzz,
rebase of a local change onto a collaborator's edit, wire-format round-trips
(positional and component forms), gitignore ancestor rules, chmod/chown and the
`.sh` default mode.

End-to-end scenarios:

```sh
dev-server/bin/dev.sh start
cd overleaf/services/web && yarn node scripts/dev-fixture.mjs   # prints PROJECT_ID=
node dev-server/bin/scenarios.mjs --project <id>
```

`dev-server/bin/scenarios.mjs` starts and stops the mount itself and prints the
evidence for each case:

| Scenario | Proves |
| --- | --- |
| mount starts / lists / content | the project image is correct before any I/O |
| write on the mount | local `write(2)` reaches document-updater |
| peer edit | a collaborator's edit appears on the mount |
| concurrent edits | both edits survive, transformed by the server |
| same-sentence rewrite | both rewrites survive and the document stays parseable |
| `.gitignore` | matched files stay local; the server tree does not grow |
| `.sh` mode, chmod, uid/gid | executable default, in-memory chmod, ownership |
| `SIGTERM` | the mount is unmounted |
| no websocket | the mount still starts and serves correct content |
| real-time outage | an edit made while real-time is down is delivered after reconnect |

`dev-server/bin/peer.mjs` is a standalone "browser" that joins a document and
applies an edit; `peer-lib.mjs` holds the shared session/socket code (including
the socket.io 0.9 heartbeat, without which the server drops idle clients).

## Development stack

`dev-server/bin/dev.sh` runs the services natively (Docker only for
mongo/redis/postgres) on the canonical CE ports: web **3000**, document-updater
3003, docstore 3016, chat 3010, filestore 3009, notifications 3042, real-time
3026, history-v1 3100, project-history 3054.

```sh
dev-server/bin/dev.sh start            # whole stack
dev-server/bin/dev.sh start real-time  # one service
dev-server/bin/dev.sh stop real-time
dev-server/bin/dev.sh restart <svc>
dev-server/bin/dev.sh status
```

Notes for this environment:

* `web` must listen on **3000**: document-updater, real-time and project-history
  call back into its private API there.
* Services are started with `setsid`, each in its own process group, so
  stopping one cannot take down the rest.
* The mongo container runs a single-node replica set named `overleaf`. If it
  comes up without a primary (unclean stop), `dev.sh start` force-restores the
  one-member config.
* `notifications` does not boot in this environment (module resolution failure);
  it is not needed by the mount.
* The fixture writes the user and tree directly (document ids are required before
  any document exists) and then hands each document to document-updater, so
  redis, docstore and mongo agree — which is also what makes web's private
  document API accept writes.

## Licence

AGPL-3.0-or-later, matching Overleaf CE.

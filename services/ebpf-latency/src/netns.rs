//! Resolve a Kubernetes algo pod's network-namespace path on the node.
//!
//! The sandbox-orchestrator launches the capture as a per-pod Job and passes the
//! algo pod's UID (the Kubernetes API exposes the container ID but not the host
//! PID, and the orchestrator can't reach the node's container runtime). Running
//! on the node with `hostPID`, the capture finds a process whose cgroup belongs
//! to that pod and returns `/proc/<pid>/ns/net`.
//!
//! The match is intentionally tolerant of cgroup-driver differences: the pod UID
//! appears in the cgroup path as `pod<uid>` (cgroupfs driver, dashes kept) or
//! `pod<uid-with-underscores>.slice` (systemd driver), and under cgroup v1 vs v2
//! the layout differs. Normalizing both sides to lowercase alphanumerics and
//! substring-matching the 32-hex UID is robust across all of these.

use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Result};

/// Resolve the netns path for the pod with the given UID (and optional container
/// id, e.g. `containerd://<hex>`), scanning host `/proc`.
pub fn resolve_netns_path(pod_uid: &str, container_id: Option<&str>) -> Result<PathBuf> {
    let pid = resolve_pod_pid(pod_uid, container_id)?;
    Ok(PathBuf::from(format!("/proc/{pid}/ns/net")))
}

fn resolve_pod_pid(pod_uid: &str, container_id: Option<&str>) -> Result<u32> {
    let want_uid = normalize(pod_uid);
    if want_uid.is_empty() {
        return Err(anyhow!("empty pod uid"));
    }
    let want_cid = container_id
        .map(strip_scheme)
        .map(|c| normalize(&c))
        .filter(|c| !c.is_empty());

    let mut best: Option<u32> = None;
    for entry in fs::read_dir("/proc")? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let Ok(contents) = fs::read_to_string(format!("/proc/{pid}/cgroup")) else {
            continue;
        };
        if cgroup_matches(&contents, &want_uid, want_cid.as_deref()) {
            // Lowest PID is the most stable choice (typically the container's
            // init); any process in the cgroup shares the same netns anyway.
            best = Some(best.map_or(pid, |b| b.min(pid)));
        }
    }
    best.ok_or_else(|| anyhow!("no process found on this node for pod uid {pod_uid}"))
}

/// Lowercase, keep only ascii-alphanumerics. Strips the `-`/`_` that distinguish
/// the cgroupfs and systemd renderings of the pod UID so both forms match.
fn normalize(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Strip a `scheme://` prefix (`containerd://abc` -> `abc`).
fn strip_scheme(s: &str) -> String {
    match s.split_once("://") {
        Some((_, rest)) => rest.to_string(),
        None => s.to_string(),
    }
}

/// True if the cgroup file belongs to the pod (and the container, if given).
fn cgroup_matches(contents: &str, want_uid_norm: &str, want_cid_norm: Option<&str>) -> bool {
    let norm = normalize(contents);
    if !norm.contains(want_uid_norm) {
        return false;
    }
    match want_cid_norm {
        Some(cid) => norm.contains(cid),
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UID: &str = "5f3a9b2c-1d4e-4f6a-8b7c-0e1d2f3a4b5c";
    const CID: &str =
        "containerd://9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b";

    #[test]
    fn matches_cgroup_v2_systemd_driver() {
        // systemd driver: dashes in the UID become underscores, `.slice` suffix.
        let line = "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod5f3a9b2c_1d4e_4f6a_8b7c_0e1d2f3a4b5c.slice/cri-containerd-9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b.scope\n";
        assert!(cgroup_matches(line, &normalize(UID), None));
        assert!(cgroup_matches(
            line,
            &normalize(UID),
            Some(&normalize(&strip_scheme(CID)))
        ));
    }

    #[test]
    fn matches_cgroup_v1_cgroupfs_driver() {
        // cgroupfs driver: dashes kept, no `.slice`, container id as a path segment.
        let contents = "11:devices:/kubepods/besteffort/pod5f3a9b2c-1d4e-4f6a-8b7c-0e1d2f3a4b5c/9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b\n10:memory:/kubepods/besteffort/pod5f3a9b2c-1d4e-4f6a-8b7c-0e1d2f3a4b5c/9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b\n";
        assert!(cgroup_matches(contents, &normalize(UID), None));
    }

    #[test]
    fn rejects_unrelated_pod() {
        let line = "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-podffffffff_0000_0000_0000_000000000000.slice/cri-containerd-deadbeef.scope\n";
        assert!(!cgroup_matches(line, &normalize(UID), None));
    }

    #[test]
    fn uid_matches_but_wrong_container_is_rejected() {
        let line = "0::/kubepods.slice/.../kubepods-besteffort-pod5f3a9b2c_1d4e_4f6a_8b7c_0e1d2f3a4b5c.slice/cri-containerd-0000000000000000.scope\n";
        assert!(cgroup_matches(line, &normalize(UID), None)); // uid still matches
        assert!(!cgroup_matches(line, &normalize(UID), Some("9a8b7c6d"))); // wrong container
    }

    #[test]
    fn strip_scheme_and_normalize() {
        assert_eq!(strip_scheme("containerd://abc"), "abc");
        assert_eq!(strip_scheme("docker://DEF"), "DEF");
        assert_eq!(strip_scheme("plain"), "plain");
        assert_eq!(normalize("5f3a-9b2c_1D"), "5f3a9b2c1d");
    }
}

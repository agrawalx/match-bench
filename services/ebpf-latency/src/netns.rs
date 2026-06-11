//! This module implements netns behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Result};

/// resolve_netns_path performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn resolve_netns_path(pod_uid: &str, container_id: Option<&str>) -> Result<PathBuf> {
    let pid = resolve_pod_pid(pod_uid, container_id)?;
    Ok(PathBuf::from(format!("/proc/{pid}/ns/net")))
}

/// resolve_pod_pid performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
            best = Some(best.map_or(pid, |b| b.min(pid)));
        }
    }
    best.ok_or_else(|| anyhow!("no process found on this node for pod uid {pod_uid}"))
}

/// normalize performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn normalize(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// strip_scheme performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn strip_scheme(s: &str) -> String {
    match s.split_once("://") {
        Some((_, rest)) => rest.to_string(),
        None => s.to_string(),
    }
}

/// cgroup_matches performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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
    /// matches_cgroup_v2_systemd_driver performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn matches_cgroup_v2_systemd_driver() {
        let line = "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod5f3a9b2c_1d4e_4f6a_8b7c_0e1d2f3a4b5c.slice/cri-containerd-9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b.scope\n";
        assert!(cgroup_matches(line, &normalize(UID), None));
        assert!(cgroup_matches(
            line,
            &normalize(UID),
            Some(&normalize(&strip_scheme(CID)))
        ));
    }

    #[test]
    /// matches_cgroup_v1_cgroupfs_driver performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn matches_cgroup_v1_cgroupfs_driver() {
        let contents = "11:devices:/kubepods/besteffort/pod5f3a9b2c-1d4e-4f6a-8b7c-0e1d2f3a4b5c/9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b\n10:memory:/kubepods/besteffort/pod5f3a9b2c-1d4e-4f6a-8b7c-0e1d2f3a4b5c/9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b\n";
        assert!(cgroup_matches(contents, &normalize(UID), None));
    }

    #[test]
    /// rejects_unrelated_pod performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn rejects_unrelated_pod() {
        let line = "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-podffffffff_0000_0000_0000_000000000000.slice/cri-containerd-deadbeef.scope\n";
        assert!(!cgroup_matches(line, &normalize(UID), None));
    }

    #[test]
    /// uid_matches_but_wrong_container_is_rejected performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn uid_matches_but_wrong_container_is_rejected() {
        let line = "0::/kubepods.slice/.../kubepods-besteffort-pod5f3a9b2c_1d4e_4f6a_8b7c_0e1d2f3a4b5c.slice/cri-containerd-0000000000000000.scope\n";
        assert!(cgroup_matches(line, &normalize(UID), None));
        assert!(!cgroup_matches(line, &normalize(UID), Some("9a8b7c6d")));
    }

    #[test]
    /// strip_scheme_and_normalize performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn strip_scheme_and_normalize() {
        assert_eq!(strip_scheme("containerd://abc"), "abc");
        assert_eq!(strip_scheme("docker://DEF"), "DEF");
        assert_eq!(strip_scheme("plain"), "plain");
        assert_eq!(normalize("5f3a-9b2c_1D"), "5f3a9b2c1d");
    }
}

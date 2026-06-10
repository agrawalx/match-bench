//! Privileged MTU clamp integration test.
//!
//! Exercises the REAL SIOCSIFMTU path (src/mtu.rs, included below via #[path])
//! against a real veth pair set to the EKS jumbo default of 9001: the clamp
//! must lower it to 1500, leave an already-small MTU alone (never raise), and
//! honor clamp=0 (disabled). This is the same code attach_programs runs inside
//! the algo pod's netns.
//!
//! Run (needs root for veth creation + SIOCSIFMTU):
//!   IICPC_REAL_EBPF_STRICT=1 sudo -E env "PATH=$PATH" \
//!     cargo test -p iicpc-ebpf-latency --test mtu_clamp -- --ignored --nocapture

#[path = "../src/mtu.rs"]
mod mtu;

use std::process::Command;

use anyhow::{bail, Context, Result};

#[test]
#[ignore = "requires root (veth creation + SIOCSIFMTU) and iproute2"]
fn clamps_jumbo_veth_down_but_never_up() -> Result<()> {
    if !require_root()? || !require_command("ip")? {
        return Ok(());
    }

    let fixture = VethFixture::create()?;
    let (jumbo, small) = (fixture.jumbo.as_str(), fixture.small.as_str());

    // EKS case: 9001 -> 1500.
    assert_eq!(mtu::get_iface_mtu(jumbo)?, 9001, "fixture MTU not applied");
    mtu::clamp_to(jumbo, 1500);
    assert_eq!(mtu::get_iface_mtu(jumbo)?, 1500, "jumbo MTU not clamped");

    // Idempotent: clamping again is a no-op.
    mtu::clamp_to(jumbo, 1500);
    assert_eq!(mtu::get_iface_mtu(jumbo)?, 1500);

    // Never raise: a deliberately small MTU stays put.
    mtu::set_iface_mtu(small, 1200).context("set small peer MTU")?;
    mtu::clamp_to(small, 1500);
    assert_eq!(
        mtu::get_iface_mtu(small)?,
        1200,
        "small MTU must not be raised"
    );

    // clamp=0 disables: the jumbo side keeps whatever it has.
    mtu::set_iface_mtu(jumbo, 9001).context("restore jumbo MTU")?;
    mtu::clamp_to(jumbo, 0);
    assert_eq!(
        mtu::get_iface_mtu(jumbo)?,
        9001,
        "clamp=0 must not touch the MTU"
    );

    Ok(())
}

// ---- veth fixture -------------------------------------------------------------

/// A host-namespace veth pair; `jumbo` starts at MTU 9001 (both veth peers must
/// allow it, so the pair is created at 9001 and `small` is lowered per-test).
/// Names stay under IFNAMSIZ (pid is at most 7 digits).
struct VethFixture {
    jumbo: String,
    small: String,
}

impl VethFixture {
    fn create() -> Result<Self> {
        let pid = std::process::id();
        let fixture = Self {
            jumbo: format!("mtuA{pid}"),
            small: format!("mtuB{pid}"),
        };
        run_ip(&[
            "link",
            "add",
            &fixture.jumbo,
            "mtu",
            "9001",
            "type",
            "veth",
            "peer",
            "name",
            &fixture.small,
            "mtu",
            "9001",
        ])?;
        Ok(fixture)
    }
}

impl Drop for VethFixture {
    fn drop(&mut self) {
        // Deleting one end removes the pair.
        let _ = Command::new("ip")
            .args(["link", "del", &self.jumbo])
            .status();
    }
}

// ---- env gating (matches tests/real_ebpf.rs) ----------------------------------

fn require_root() -> Result<bool> {
    if unsafe { libc::geteuid() } == 0 {
        return Ok(true);
    }
    skip_or_fail("MTU clamp integration test requires root privileges")
}

fn require_command(command: &str) -> Result<bool> {
    let exists = Command::new(command)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok();
    if exists {
        Ok(true)
    } else {
        skip_or_fail(format!(
            "{command} is required for MTU clamp integration test"
        ))
    }
}

fn skip_or_fail(reason: impl AsRef<str>) -> Result<bool> {
    let reason = reason.as_ref();
    if std::env::var_os("IICPC_REAL_EBPF_STRICT").is_some() {
        bail!("{reason}");
    }
    eprintln!("skipping MTU clamp integration test: {reason}");
    Ok(false)
}

fn run_ip(args: &[&str]) -> Result<()> {
    let status = Command::new("ip")
        .args(args)
        .status()
        .with_context(|| format!("run ip {}", args.join(" ")))?;
    if status.success() {
        Ok(())
    } else {
        bail!("ip {} failed with {status}", args.join(" "))
    }
}

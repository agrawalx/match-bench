//! This module defines tests for mtu clamp.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

#[path = "../src/mtu.rs"]
mod mtu;

use std::process::Command;

use anyhow::{bail, Context, Result};

#[test]
#[ignore = "requires root (veth creation + SIOCSIFMTU) and iproute2"]
/// clamps_jumbo_veth_down_but_never_up performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn clamps_jumbo_veth_down_but_never_up() -> Result<()> {
    if !require_root()? || !require_command("ip")? {
        return Ok(());
    }

    let fixture = VethFixture::create()?;
    let (jumbo, small) = (fixture.jumbo.as_str(), fixture.small.as_str());

    assert_eq!(mtu::get_iface_mtu(jumbo)?, 9001, "fixture MTU not applied");
    mtu::clamp_to(jumbo, 1500);
    assert_eq!(mtu::get_iface_mtu(jumbo)?, 1500, "jumbo MTU not clamped");

    mtu::clamp_to(jumbo, 1500);
    assert_eq!(mtu::get_iface_mtu(jumbo)?, 1500);

    mtu::set_iface_mtu(small, 1200).context("set small peer MTU")?;
    mtu::clamp_to(small, 1500);
    assert_eq!(
        mtu::get_iface_mtu(small)?,
        1200,
        "small MTU must not be raised"
    );

    mtu::set_iface_mtu(jumbo, 9001).context("restore jumbo MTU")?;
    mtu::clamp_to(jumbo, 0);
    assert_eq!(
        mtu::get_iface_mtu(jumbo)?,
        9001,
        "clamp=0 must not touch the MTU"
    );

    Ok(())
}

/// VethFixture stores the state passed across this module boundary.
/// Keep field changes compatible with callers and serialized contracts.
struct VethFixture {
    jumbo: String,
    small: String,
}

impl VethFixture {
    /// create performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
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
    /// drop performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn drop(&mut self) {
        let _ = Command::new("ip")
            .args(["link", "del", &self.jumbo])
            .status();
    }
}

/// require_root performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn require_root() -> Result<bool> {
    if unsafe { libc::geteuid() } == 0 {
        return Ok(true);
    }
    skip_or_fail("MTU clamp integration test requires root privileges")
}

/// require_command performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

/// skip_or_fail performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn skip_or_fail(reason: impl AsRef<str>) -> Result<bool> {
    let reason = reason.as_ref();
    if std::env::var_os("IICPC_REAL_EBPF_STRICT").is_some() {
        bail!("{reason}");
    }
    eprintln!("skipping MTU clamp integration test: {reason}");
    Ok(false)
}

/// run_ip performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
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

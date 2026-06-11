//! This module implements mtu behavior.
//!
//! It belongs to the IICPC benchmarking platform and should keep its
//! behavior consistent with the service contracts documented in design.md.
//! The comments in this file describe public structure and callable behavior.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

/// clamp_to performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn clamp_to(iface: &str, clamp: usize) {
    if clamp == 0 {
        info!(iface, "capture MTU clamp disabled (CAPTURE_CLAMP_MTU=0)");
        return;
    }
    match clamp_iface_mtu(iface, clamp) {
        Ok(Some((old, new))) => {
            info!(
                iface,
                old_mtu = old,
                new_mtu = new,
                "clamped capture interface MTU"
            );
        }
        Ok(None) => {
            info!(
                iface,
                clamp, "capture interface MTU already within clamp; unchanged"
            );
        }
        Err(err) => {
            warn!(
                iface,
                clamp,
                error = %err,
                "FAILED to clamp capture interface MTU; super-MTU segments will be truncated and reset flows (orders.acked loss likely)"
            );
        }
    }
}

/// clamp_iface_mtu performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn clamp_iface_mtu(iface: &str, clamp: usize) -> Result<Option<(libc::c_int, libc::c_int)>> {
    let current = get_iface_mtu(iface)?;
    let Some(target) = mtu_clamp_target(current, clamp) else {
        return Ok(None);
    };
    set_iface_mtu(iface, target)?;
    Ok(Some((current, target)))
}

/// mtu_clamp_target performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn mtu_clamp_target(current: libc::c_int, clamp: usize) -> Option<libc::c_int> {
    if clamp == 0 {
        return None;
    }
    let clamp = libc::c_int::try_from(clamp).ok()?;
    (current > clamp).then_some(clamp)
}

/// get_iface_mtu performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn get_iface_mtu(iface: &str) -> Result<libc::c_int> {
    let sock = ioctl_socket()?;
    let mut ifr = ifreq_for(iface)?;
    let rc = unsafe { libc::ioctl(sock.as_raw_fd(), libc::SIOCGIFMTU as _, &mut ifr) };
    if rc != 0 {
        return Err(io::Error::last_os_error()).with_context(|| format!("SIOCGIFMTU({iface})"));
    }
    Ok(unsafe { ifr.ifr_ifru.ifru_mtu })
}

/// set_iface_mtu performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn set_iface_mtu(iface: &str, mtu: libc::c_int) -> Result<()> {
    let sock = ioctl_socket()?;
    let mut ifr = ifreq_for(iface)?;
    ifr.ifr_ifru.ifru_mtu = mtu;
    let rc = unsafe { libc::ioctl(sock.as_raw_fd(), libc::SIOCSIFMTU as _, &mut ifr) };
    if rc != 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("SIOCSIFMTU({iface}, {mtu})"));
    }
    Ok(())
}

/// ioctl_socket performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
fn ioctl_socket() -> Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error()).context("open AF_INET ioctl socket");
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// ifreq_for performs the module-specific operation described by its name.
/// It keeps validation, side effects, and returned values within this module's contract.
pub fn ifreq_for(iface: &str) -> Result<libc::ifreq> {
    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    let bytes = iface.as_bytes();
    if bytes.is_empty() || bytes.len() >= ifr.ifr_name.len() {
        bail!(
            "interface name {iface:?} does not fit IFNAMSIZ ({} bytes)",
            ifr.ifr_name.len()
        );
    }
    for (dst, &src) in ifr.ifr_name.iter_mut().zip(bytes) {
        *dst = src as libc::c_char;
    }
    Ok(ifr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    /// mtu_clamp_target_only_clamps_downward performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn mtu_clamp_target_only_clamps_downward() {
        let cases: &[(libc::c_int, usize, Option<libc::c_int>)] = &[
            (9001, 1500, Some(1500)),
            (1500, 1500, None),
            (1400, 1500, None),
            (9001, 0, None),
            (65536, 9000, Some(9000)),
            (1500, usize::MAX, None),
        ];
        for &(current, clamp, want) in cases {
            assert_eq!(
                mtu_clamp_target(current, clamp),
                want,
                "current={current} clamp={clamp}"
            );
        }
    }

    #[test]
    /// ifreq_for_copies_name_nul_terminated performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ifreq_for_copies_name_nul_terminated() {
        let ifr = ifreq_for("eth0").unwrap();
        let name: Vec<u8> = ifr.ifr_name.iter().map(|&c| c as u8).collect();
        assert_eq!(&name[..5], b"eth0\0");
        assert!(
            name[5..].iter().all(|&b| b == 0),
            "trailing bytes not zeroed"
        );
    }

    #[test]
    /// ifreq_for_rejects_empty_and_oversized_names performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn ifreq_for_rejects_empty_and_oversized_names() {
        assert!(ifreq_for("").is_err());
        assert!(ifreq_for("0123456789abcdef").is_err());
        assert!(ifreq_for("0123456789abcde").is_ok());
    }

    #[test]
    /// get_iface_mtu_reads_loopback performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn get_iface_mtu_reads_loopback() {
        let mtu = get_iface_mtu("lo").expect("read lo MTU");
        assert!(mtu >= 68, "implausible loopback MTU {mtu}");
    }

    #[test]
    /// get_iface_mtu_fails_for_missing_interface performs the module-specific operation described by its name.
    /// It keeps validation, side effects, and returned values within this module's contract.
    fn get_iface_mtu_fails_for_missing_interface() {
        assert!(get_iface_mtu("iicpc-no-such0").is_err());
    }
}

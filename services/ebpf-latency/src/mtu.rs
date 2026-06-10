//! Capture-interface MTU clamp (SIOCGIFMTU/SIOCSIFMTU).
//!
//! The kernel capture (src/ebpf.rs) copies at most CAPTURE_CAP = 1536 payload
//! bytes per TCP segment; anything larger is truncated and forces a lossy flow
//! reset in the userspace reassembler (TRUNCATED_CAPTURES). With offloads
//! disabled the segment size is bounded by the interface MTU — but on EKS the
//! pod veth inherits the node ENI's 9001-byte jumbo MTU, so even single
//! un-coalesced segments can exceed the capture window and silently corrupt
//! flows. Clamping the MTU to CAPTURE_CLAMP_MTU (default 1500; 0 disables)
//! restores the one-packet=one-order capture contract.
//!
//! Implemented with raw ioctls through libc so it works in the slim capture
//! image (no dependency on iproute2 being installed) and inside the algo pod's
//! netns, where attach_programs already runs. Like disable_offloads, this is
//! best-effort: failure is loud but never aborts the capture, since a
//! degraded-but-running measurement beats none. The clamp only ever LOWERS the
//! MTU (old -> new is logged); a deliberately small MTU is left alone.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use anyhow::{bail, Context, Result};
use tracing::{info, warn};

/// Clamp `iface`'s MTU down to `clamp` bytes (0 disables). Reads the current
/// MTU first (SIOCGIFMTU) and only ever lowers it. Never returns an error:
/// failures are logged loudly because an unclamped jumbo MTU means truncated
/// captures and lost orders.acked events, but the capture itself must still run.
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

/// Read-decide-set. Returns Ok(Some((old, new))) when the MTU was lowered,
/// Ok(None) when no change was needed (already <= clamp).
fn clamp_iface_mtu(iface: &str, clamp: usize) -> Result<Option<(libc::c_int, libc::c_int)>> {
    let current = get_iface_mtu(iface)?;
    let Some(target) = mtu_clamp_target(current, clamp) else {
        return Ok(None);
    };
    set_iface_mtu(iface, target)?;
    Ok(Some((current, target)))
}

/// Pure clamp decision: only clamp DOWNWARD (raising the MTU could mask a
/// deliberately small configuration), and 0 disables the clamp entirely. A
/// clamp that does not fit in c_int is treated as "no action".
pub fn mtu_clamp_target(current: libc::c_int, clamp: usize) -> Option<libc::c_int> {
    if clamp == 0 {
        return None;
    }
    let clamp = libc::c_int::try_from(clamp).ok()?;
    (current > clamp).then_some(clamp)
}

/// Current MTU via SIOCGIFMTU (unprivileged).
pub fn get_iface_mtu(iface: &str) -> Result<libc::c_int> {
    let sock = ioctl_socket()?;
    let mut ifr = ifreq_for(iface)?;
    // SAFETY: SIOCGIFMTU reads ifr_name and writes ifru_mtu within the
    // kernel-defined ifreq layout; ifr lives for the whole call.
    let rc = unsafe { libc::ioctl(sock.as_raw_fd(), libc::SIOCGIFMTU as _, &mut ifr) };
    if rc != 0 {
        return Err(io::Error::last_os_error()).with_context(|| format!("SIOCGIFMTU({iface})"));
    }
    // SAFETY: the kernel populated ifru_mtu on success.
    Ok(unsafe { ifr.ifr_ifru.ifru_mtu })
}

/// Set the MTU via SIOCSIFMTU (requires CAP_NET_ADMIN in the interface's netns).
pub fn set_iface_mtu(iface: &str, mtu: libc::c_int) -> Result<()> {
    let sock = ioctl_socket()?;
    let mut ifr = ifreq_for(iface)?;
    ifr.ifr_ifru.ifru_mtu = mtu;
    // SAFETY: SIOCSIFMTU only reads from ifr; ifr lives for the whole call.
    let rc = unsafe { libc::ioctl(sock.as_raw_fd(), libc::SIOCSIFMTU as _, &mut ifr) };
    if rc != 0 {
        return Err(io::Error::last_os_error())
            .with_context(|| format!("SIOCSIFMTU({iface}, {mtu})"));
    }
    Ok(())
}

/// Any AF_INET datagram socket works as an ioctl handle; it carries no traffic.
fn ioctl_socket() -> Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error()).context("open AF_INET ioctl socket");
    }
    // SAFETY: fd is a freshly created descriptor owned by no one else.
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

/// Build a zeroed ifreq with `iface` copied into ifr_name (NUL-terminated).
pub fn ifreq_for(iface: &str) -> Result<libc::ifreq> {
    // SAFETY: ifreq is plain-old-data; all-zeroes is a valid value.
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
    fn mtu_clamp_target_only_clamps_downward() {
        // (current, clamp, expected)
        let cases: &[(libc::c_int, usize, Option<libc::c_int>)] = &[
            (9001, 1500, Some(1500)),  // EKS jumbo veth -> clamp to classic Ethernet
            (1500, 1500, None),        // already at the clamp
            (1400, 1500, None),        // never RAISE a smaller MTU
            (9001, 0, None),           // 0 disables the clamp
            (65536, 9000, Some(9000)), // loopback-sized down to a jumbo clamp
            (1500, usize::MAX, None),  // unrepresentable clamp -> no action
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
    fn ifreq_for_rejects_empty_and_oversized_names() {
        assert!(ifreq_for("").is_err());
        // 16 bytes == IFNAMSIZ leaves no room for the NUL terminator.
        assert!(ifreq_for("0123456789abcdef").is_err());
        // 15 bytes + NUL exactly fills IFNAMSIZ.
        assert!(ifreq_for("0123456789abcde").is_ok());
    }

    #[test]
    fn get_iface_mtu_reads_loopback() {
        // SIOCGIFMTU needs no privileges and lo always exists on Linux.
        let mtu = get_iface_mtu("lo").expect("read lo MTU");
        assert!(mtu >= 68, "implausible loopback MTU {mtu}");
    }

    #[test]
    fn get_iface_mtu_fails_for_missing_interface() {
        assert!(get_iface_mtu("iicpc-no-such0").is_err());
    }
}

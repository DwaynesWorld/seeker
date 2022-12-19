use crate::GIT_BRANCH;
use crate::GIT_SHA;
use crate::PKG_NAME;
use crate::PKG_VERS;
use crate::RUST_VERS;

pub fn init() -> std::io::Result<()> {
    get_cfg!(target_os: "windows", "macos", "ios", "linux", "android", "freebsd", "openbsd", "netbsd");
    get_cfg!(target_arch: "x86", "x86_64", "mips", "powerpc", "powerpc64", "arm", "aarch64");
    println!(
        "Name: {}\n\
		Release Version: {}\n\
		Target Arch: {} - {}\n\
		Rust Version: {}\n\
		Build: {} - {}\n",
        PKG_NAME,
        PKG_VERS,
        target_os(),
        target_arch(),
        RUST_VERS,
        GIT_BRANCH,
        GIT_SHA
    );
    Ok(())
}

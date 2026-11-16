//! End-to-end reverse-tunnel test: echo ← agent ← hub ← client.
//! Spawns the published `agent` binary plus local `hub` / `echo-server` helpers.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    listener.local_addr().unwrap().port()
}

fn wait_port(port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for 127.0.0.1:{port}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn bin(name: &str) -> Command {
    // Cargo sets CARGO_BIN_EXE_<name> with '-' → '_'. Some environments omit it;
    // fall back to target/debug next to the manifest.
    let key = format!("CARGO_BIN_EXE_{}", name.replace('-', "_"));
    let path = std::env::var(&key).unwrap_or_else(|_| {
        let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
        let candidate = std::path::Path::new(&manifest)
            .join("target")
            .join("debug")
            .join(name);
        if candidate.exists() {
            return candidate.to_string_lossy().into_owned();
        }
        // Workspace / alternate CARGO_TARGET_DIR
        if let Ok(target) = std::env::var("CARGO_TARGET_DIR") {
            let alt = std::path::Path::new(&target).join("debug").join(name);
            if alt.exists() {
                return alt.to_string_lossy().into_owned();
            }
        }
        panic!(
            "missing {key} and could not find debug binary `{name}` under target/debug"
        );
    });
    Command::new(path)
}

#[test]
fn reverse_tunnel_echoes_bytes() {
    let echo_port = free_port();
    let wss_port = free_port();
    let forward_port = free_port();
    let token = "ci-e2e-token";

    let _echo = ChildGuard(
        bin("echo-server")
            .env("ECHO_ADDR", format!("127.0.0.1:{echo_port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn echo-server"),
    );

    wait_port(echo_port, Duration::from_secs(5));

    let _hub = ChildGuard(
        bin("hub")
            .env("HUB_TOKEN", token)
            .env("HUB_WSS_ADDR", format!("127.0.0.1:{wss_port}"))
            .env("HUB_FORWARD_ADDR", format!("127.0.0.1:{forward_port}"))
            .env("HUB_DIAL_HOST", "127.0.0.1")
            .env("HUB_DIAL_PORT", echo_port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn hub"),
    );

    wait_port(wss_port, Duration::from_secs(5));
    wait_port(forward_port, Duration::from_secs(5));

    let _agent = ChildGuard(
        bin("agent")
            .env("POCKETBI_TOKEN", token)
            .env("POCKETBI_URL", format!("ws://127.0.0.1:{wss_port}/agent"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn agent"),
    );

    // Give the agent time to hello / authenticate.
    thread::sleep(Duration::from_millis(400));

    let mut client =
        TcpStream::connect(("127.0.0.1", forward_port)).expect("connect hub forward port");
    client
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    client
        .set_write_timeout(Some(Duration::from_secs(3)))
        .unwrap();

    let payload = b"hello-from-e2e\n";
    client.write_all(payload).expect("write");
    client.flush().ok();

    let mut buf = vec![0u8; 64];
    let n = client.read(&mut buf).expect("read echo");
    assert_eq!(&buf[..n], payload, "tunnel should echo payload bytes");
}

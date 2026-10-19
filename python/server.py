"""Start and stop the Tickflow API as a real process (used by the load generator and the crash test)."""
import json
import os
import signal
import subprocess
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
API_DLL = ROOT / "src/Tickflow.Api/bin/Release/net8.0/Tickflow.Api.dll"
DOTNET = os.environ.get("DOTNET", str(Path.home() / ".dotnet/dotnet"))


def build() -> None:
    subprocess.run([DOTNET, "build", "-c", "Release", str(ROOT / "src/Tickflow.Api")],
                   check=True, stdout=subprocess.DEVNULL)


class Server:
    def __init__(self, data_dir: Path, durability: str = "Group", port: int = 5077):
        self.data_dir, self.durability, self.port = data_dir, durability, port
        self.url = f"http://127.0.0.1:{port}"
        self.proc: subprocess.Popen | None = None

    def start(self, timeout: float = 15.0) -> dict:
        env = dict(os.environ, Tickflow__DataDir=str(self.data_dir), Tickflow__Durability=self.durability)
        self.proc = subprocess.Popen([DOTNET, str(API_DLL), "--urls", self.url], env=env,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(self.url + "/health", timeout=1) as r:
                    return json.load(r)
            except OSError:
                time.sleep(0.1)
        self.kill()
        raise RuntimeError("server did not start")

    def kill(self) -> None:
        """SIGKILL: no shutdown hooks, no flushing. The harshest crash a process can have."""
        if self.proc and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=10)

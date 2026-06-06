"""Contract tests for remote execution command construction.

A recording runner records every command while returning the same canned
outputs DryRunRunner does (PID, poll "done", exit "0", $HOME), so the whole
sync → run → tar flow runs deterministically with no real SSH and we can assert
on the exact commands issued.
"""

from pathlib import Path, PurePosixPath

from click.testing import CliRunner

from simctl import main, remote

HOST = "sukun@ethp2p"


class RecordingRunner(remote.DryRunRunner):
    def __init__(self) -> None:
        self.run_calls: list[tuple[list[str], Path | None]] = []
        self.ssh_calls: list[str] = []

    def run_cmd(self, cmd, capture_output=False, cwd=None):
        self.run_calls.append((list(cmd), cwd))
        return super().run_cmd(cmd, capture_output, cwd)

    def ssh_cmd(self, host, cmd, capture_output=False):
        self.ssh_calls.append(cmd)
        return super().ssh_cmd(host, cmd, capture_output)


def _runner() -> tuple[remote.Runner, RecordingRunner]:
    r = remote.Runner(dry_run=True)
    rec = RecordingRunner()
    r._runner = rec
    return r, rec


def test_sync_to_remote_rsync_command():
    r, rec = _runner()
    res, remote_path = r.sync_to_remote(HOST, Path("/home/sukun/dev/eth-slot-sim"))

    assert res.returncode == 0
    # $HOME resolved via the canned response → $HOME/<basename>.
    assert remote_path == PurePosixPath("/home/remote-user/eth-slot-sim")

    rsync = next(cmd for cmd, _ in rec.run_calls if cmd[0] == "rsync")
    assert "--filter=:- .gitignore" in rsync
    assert "--exclude" in rsync and ".git" in rsync and ".jj" in rsync
    assert rsync[-2] == "eth-slot-sim/"
    assert rsync[-1] == f"{HOST}:/home/remote-user/eth-slot-sim/"


def test_run_remote_simctl_starts_nohup_and_reads_exit():
    r, rec = _runner()
    cwd = PurePosixPath("/home/remote-user/eth-slot-sim")
    rc = r.run_remote_simctl(HOST, cwd, ["run", "--config", "configs/smoke.yaml"])

    assert rc == 0  # canned exit-code file is "0"
    start = rec.ssh_calls[0]
    assert "uv sync --quiet" in start
    assert "uv run simctl run --config configs/smoke.yaml" in start
    assert "nohup" in start and "echo $!" in start
    # The flow polls liveness then reads the exit-code file.
    assert any("kill -0" in c for c in rec.ssh_calls)
    assert any(c.startswith("cat ") and ".exit" in c for c in rec.ssh_calls)


def test_tar_and_cleanup_tars_then_removes():
    r, rec = _runner()
    cwd = PurePosixPath("/home/remote-user/eth-slot-sim")
    rc = r.tar_and_cleanup(HOST, cwd, "runs/smoke")

    assert rc == 0
    joined = "\n".join(rec.ssh_calls)
    assert "tar -czf" in joined
    assert "/home/remote-user/eth-slot-sim/runs/smoke.tar.gz" in joined
    assert "rm -rf -- " in joined and "runs/smoke" in joined


# --- CLI wiring: `simctl run|compare --remote --dry-run` forwards the right args ---


def test_cli_run_remote_forwards_args():
    res = CliRunner().invoke(
        main.cli,
        ["run", "--config", "configs/smoke.yaml", "--remote", HOST,
         "--dry-run", "--output-dir", "runs/mainnet"],
    )
    assert res.exit_code == 0, res.output
    assert "uv run simctl run --config configs/smoke.yaml --output-dir runs/mainnet" in res.output
    assert "rsync" in res.output and "--filter=:- .gitignore" in res.output


def test_cli_compare_remote_forwards_args():
    res = CliRunner().invoke(
        main.cli,
        ["compare", "--config", "configs/smoke.yaml", "--remote", HOST, "--dry-run"],
    )
    assert res.exit_code == 0, res.output
    assert "uv run simctl compare --config configs/smoke.yaml" in res.output

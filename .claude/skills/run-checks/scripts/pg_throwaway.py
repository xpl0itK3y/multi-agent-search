"""A throwaway PostgreSQL 16 for postgres-marked tests and the Postgres Smoke simulation.

For machines without Docker: the binaries come from the `pgserver` wheel, installed with
--no-deps into a temp directory (never into the repo or its venv). Run it with the same
Python that runs the tests, so the binaries match its platform (Linux, macOS or Windows).

    python pg_throwaway.py up [--port 55432]   install if needed, initdb, start, create the database
    python pg_throwaway.py reset-db            drop and recreate the database (a fresh one, like CI)
    python pg_throwaway.py env                 the variables the tests and the smoke need
    python pg_throwaway.py status | down       server status | stop the server
    python pg_throwaway.py destroy             stop and delete the whole directory

The server listens on 127.0.0.1 only, as user/password app/app (the CI job's credentials),
with fsync off: it is disposable. State lives in <tempdir>/mas-pg unless --dir is given.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

USER = PASSWORD = "app"
DATABASE = "multi_agent_search"
# On Windows every child gets no console window (each one would flash on the desktop).
NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def run(args: list[str], env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(
        args, capture_output=True, text=True, env=env, stdin=subprocess.DEVNULL, creationflags=NO_WINDOW
    )
    if check and result.returncode != 0:
        sys.exit(f"{Path(args[0]).name} failed ({result.returncode}):\n{result.stdout[-1500:]}{result.stderr[-1500:]}")
    return result


def bin_dir(root: Path) -> Path:
    return root / "wheel" / "pgserver" / "pginstall" / "bin"


def tool(root: Path, name: str) -> str:
    suffix = ".exe" if os.name == "nt" else ""
    return str(bin_dir(root) / f"{name}{suffix}")


def pg_env(root: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["PATH"] = str(bin_dir(root)) + os.pathsep + env.get("PATH", "")
    # Some Windows setups default the client encoding to the ANSI code page (e.g. WIN1251).
    env["PGCLIENTENCODING"] = "UTF8"
    return env


def install(root: Path) -> None:
    if bin_dir(root).is_dir():
        return
    print("installing the pgserver wheel into", root / "wheel")
    run([sys.executable, "-m", "pip", "install", "--quiet", "--no-deps", "--target", str(root / "wheel"), "pgserver"])
    if not bin_dir(root).is_dir():
        sys.exit("pgserver installed, but it has no pginstall/bin for this platform")


def initdb(root: Path) -> None:
    if (root / "data" / "PG_VERSION").exists():
        return
    pwfile = root / "pw.txt"
    pwfile.write_text(PASSWORD + "\n", encoding="utf-8")
    run(
        [tool(root, "initdb"), "-D", str(root / "data"), "-U", USER, f"--pwfile={pwfile}",
         "-A", "scram-sha-256", "-E", "UTF8", "--locale=C"],
        env=pg_env(root),
    )
    pwfile.unlink()


def is_running(root: Path) -> bool:
    if not (root / "data").is_dir():
        return False
    return run([tool(root, "pg_ctl"), "-D", str(root / "data"), "status"], env=pg_env(root), check=False).returncode == 0


def start(root: Path, port: int) -> None:
    if is_running(root):
        print("already running")
        return
    options = f"-p {port} -c listen_addresses=127.0.0.1 -c max_connections=200 -c fsync=off"
    # Not through pipes: on Windows the server inherits pg_ctl's stdout/stderr handles, so a
    # captured run would wait for their EOF for as long as the server lives.
    out = root / "pg_ctl.log"
    with out.open("w", encoding="utf-8") as sink:
        code = subprocess.run(
            [tool(root, "pg_ctl"), "-D", str(root / "data"), "-l", str(root / "server.log"), "-o", options, "-w", "start"],
            stdout=sink, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=pg_env(root), creationflags=NO_WINDOW,
        ).returncode
    if code != 0:
        sys.exit(f"pg_ctl start failed ({code}):\n{out.read_text(encoding='utf-8', errors='replace')[-1500:]}"
                 f"\nsee {root / 'server.log'}")
    (root / "port").write_text(str(port), encoding="utf-8")


def stop(root: Path) -> None:
    if is_running(root):
        run([tool(root, "pg_ctl"), "-D", str(root / "data"), "-m", "fast", "-w", "stop"], env=pg_env(root))
        print("stopped")


def port_of(root: Path, default: int) -> int:
    try:
        return int((root / "port").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def recreate_database(port: int, drop: bool) -> None:
    import psycopg  # the app's own driver, from requirements.lock

    os.environ["PGCLIENTENCODING"] = "UTF8"
    with psycopg.connect(
        host="127.0.0.1", port=port, user=USER, password=PASSWORD, dbname="postgres", autocommit=True
    ) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DATABASE,)).fetchone()
        if exists and drop:
            conn.execute(f"DROP DATABASE {DATABASE} WITH (FORCE)")
            exists = None
        if not exists:
            conn.execute(f"CREATE DATABASE {DATABASE}")
    print(f"database {DATABASE} ready on 127.0.0.1:{port}")


def print_env(port: int) -> None:
    values = {
        "TASK_STORE_BACKEND": "postgres",
        "POSTGRES_HOST": "127.0.0.1",
        "POSTGRES_PORT": str(port),
        "POSTGRES_USER": USER,
        "POSTGRES_PASSWORD": PASSWORD,
        "POSTGRES_DB": DATABASE,
        "PGCLIENTENCODING": "UTF8",
    }
    for key, value in values.items():
        print(f"export {key}={value}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["up", "reset-db", "env", "status", "down", "destroy"])
    parser.add_argument("--port", type=int, help="default: the port it was started on, else 55432")
    parser.add_argument("--dir", type=Path, default=Path(tempfile.gettempdir()) / "mas-pg")
    args = parser.parse_args()
    root: Path = args.dir.resolve()
    port = args.port or port_of(root, 55432)

    if args.command == "up":
        root.mkdir(parents=True, exist_ok=True)
        install(root)
        initdb(root)
        start(root, port)
        recreate_database(port, drop=False)
        print_env(port)
    elif args.command == "reset-db":
        recreate_database(port, drop=True)
    elif args.command == "env":
        print_env(port)
    elif args.command == "status":
        print(f"running on 127.0.0.1:{port}" if is_running(root) else "not running", f"({root})")
    elif args.command == "down":
        stop(root)
    elif args.command == "destroy":
        stop(root)
        shutil.rmtree(root, ignore_errors=True)
        print("deleted", root)


if __name__ == "__main__":
    main()

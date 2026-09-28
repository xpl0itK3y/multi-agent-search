"""docker-compose.yml keeps the settings the workers and the exposed ports depend on.

Parsed with PyYAML's safe_load (which resolves `<<` merge keys like Compose does), so
no Docker is needed. A merge key is shallow: a worker that declares its own
`environment:` replaces the shared one wholesale, which once left every worker
without REDIS_URL / USE_REDIS_BROKER / LOG_FORMAT (polling Postgres while the API
pushed job ids to Redis, logging plain text that promtail could not parse).
"""
import re
from pathlib import Path

import pytest
import yaml

COMPOSE_FILE = Path(__file__).resolve().parents[1] / "docker-compose.yml"
WORKERS = ("worker", "worker_2", "worker_3")
# The one service meant to be reachable from outside the host.
PUBLIC_SERVICES = {"web"}


def load_services(path: Path = COMPOSE_FILE) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["services"]


def effective_environment(service: dict) -> dict[str, str]:
    """The service's `environment:` as a mapping (list form `KEY=VALUE` included)."""
    environment = service.get("environment") or {}
    if isinstance(environment, list):
        environment = dict(item.split("=", 1) if "=" in item else (item, "") for item in environment)
    return {str(key): str(value) for key, value in environment.items()}


def worker_services(services: dict) -> dict[str, dict]:
    return {
        name: service
        for name, service in services.items()
        if "run_finalize_worker.py" in str(service.get("command", ""))
    }


def published_host_ips(service: dict) -> list[str]:
    """The host address of each published port ("" when bound to every interface)."""
    host_ips = []
    for port in service.get("ports") or []:
        if isinstance(port, dict):  # long syntax
            host_ips.append(str(port.get("host_ip", "")))
            continue
        # ${API_PORT:-8001} interpolation has colons of its own.
        parts = re.sub(r"\$\{[^}]*\}", "0", str(port)).split(":")
        # HOST_IP:HOST:CONTAINER has three parts; HOST:CONTAINER and CONTAINER bind 0.0.0.0.
        host_ips.append(parts[0] if len(parts) == 3 else "")
    return host_ips


@pytest.fixture(scope="module")
def services():
    return load_services()


def test_every_expected_worker_runs_the_worker_script(services):
    assert set(WORKERS) <= set(worker_services(services))


@pytest.mark.parametrize("worker", WORKERS)
def test_workers_inherit_the_broker_and_json_logging(services, worker):
    environment = effective_environment(services[worker])

    assert environment.get("REDIS_URL"), f"{worker} has no REDIS_URL"
    assert environment.get("USE_REDIS_BROKER") == "true"
    assert environment.get("LOG_FORMAT") == "json"


def test_worker_names_and_metrics_ports_are_distinct(services):
    environments = [effective_environment(service) for service in worker_services(services).values()]
    names = [environment.get("WORKER_NAME") for environment in environments]
    ports = [environment.get("WORKER_METRICS_PORT") for environment in environments]

    assert all(names) and len(set(names)) == len(names)
    assert all(ports) and len(set(ports)) == len(ports)


def test_only_the_web_proxy_is_published_beyond_loopback(services):
    exposed = {}
    for name, service in services.items():
        host_ips = published_host_ips(service)
        if name not in PUBLIC_SERVICES and any(host_ip != "127.0.0.1" for host_ip in host_ips):
            exposed[name] = host_ips

    assert exposed == {}


NGINX_HTML = "/usr/share/nginx/html"  # web/nginx.conf's root; web/Dockerfile copies the build there


def mount_targets(service: dict) -> list[tuple[str, object]]:
    """(container path, entry) for each of the service's volumes, short (`src:dst[:mode]`,
    or a bare `dst`) or long syntax."""
    targets = []
    for volume in service.get("volumes") or []:
        if isinstance(volume, dict):  # long syntax
            targets.append((str(volume.get("target", "")), volume))
            continue
        # ${WEB_DIST:-./web/dist} interpolation and a Windows drive (C:\web\dist) carry colons
        # of their own; neither is the container path.
        spec = re.sub(r"^[A-Za-z]:(?=[\\/])", "0", re.sub(r"\$\{[^}]*\}", "0", str(volume)))
        parts = spec.split(":")
        targets.append((parts[1] if len(parts) > 1 else parts[0], volume))
    return targets


def hides(target: str, path: str) -> bool:
    """A mount at target covers path: the path itself, a directory above it, or one inside."""
    target, path = target.rstrip("/") or "/", path.rstrip("/")
    return target == path or path.startswith(target.rstrip("/") + "/") or target.startswith(path + "/")


def test_web_serves_the_bundle_its_image_built(services):
    """web/Dockerfile builds the SPA into the image. A bind mount of ./web/dist over nginx's
    html dir shadowed that build with whatever was last built on the host: rebuilding the
    image changed nothing, and the stack kept serving a weeks-old UI (one whose public
    report could not scroll)."""
    web = services["web"]
    shadowing = [entry for target, entry in mount_targets(web) if hides(target, NGINX_HTML)]
    assert shadowing == [], (
        f"web mounts {shadowing}, hiding the SPA its image built into {NGINX_HTML} (web/Dockerfile);"
        " drop the mount and rebuild with `docker compose up -d --build web`"
    )
    assert web["build"]["context"] == "./web"


@pytest.mark.parametrize(
    ("volume", "shadows"),
    [
        ("./web/dist:/usr/share/nginx/html:ro", True),
        ("./web/dist:/usr/share/nginx/html/", True),
        ("${WEB_DIST:-./web/dist}:/usr/share/nginx/html:ro", True),
        ("C:\\web\\dist:/usr/share/nginx/html", True),
        ("./web:/usr/share/nginx", True),
        ("./assets:/usr/share/nginx/html/assets", True),
        ("html-volume:/usr/share/nginx/html", True),
        ({"type": "bind", "source": "./web/dist", "target": "/usr/share/nginx/html"}, True),
        ("/usr/share/nginx/html", True),
        ("./logs:/var/log/nginx", False),
        ("./web/nginx.conf:/etc/nginx/conf.d/default.conf:ro", False),
        ({"type": "volume", "source": "cache", "target": "/var/cache/nginx"}, False),
    ],
)
def test_the_mount_check_sees_every_way_to_cover_the_html_dir(volume, shadows):
    assert any(hides(target, NGINX_HTML) for target, _ in mount_targets({"volumes": [volume]})) is shadows

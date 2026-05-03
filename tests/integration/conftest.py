"""Integration test fixtures — requires Docker services (UC + MinIO)."""

import pytest


@pytest.fixture(scope="session")
def docker_services_available():
    """Check if Docker Compose services are running.

    Skip integration tests if services are not available.
    """
    import socket

    def _is_port_open(host: str, port: int) -> bool:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            return False

    if not _is_port_open("localhost", 8080):
        pytest.skip("Unity Catalog server not running (port 8080)")
    if not _is_port_open("localhost", 9000):
        pytest.skip("MinIO not running (port 9000)")

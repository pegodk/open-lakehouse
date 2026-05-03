"""Integration test for bronze asset materialization."""

import pytest


@pytest.mark.integration
class TestBronzeAssets:
    """Integration tests requiring Docker services (UC + MinIO)."""

    def test_bronze_orders_materializes(self, docker_services_available) -> None:
        """Placeholder: verify bronze_orders asset runs against real UC."""
        # TODO: Implement once Docker Compose infra is verified
        # 1. Write sample CSV to MinIO
        # 2. Materialize bronze_orders via Dagster
        # 3. Read back from Delta and verify
        pass

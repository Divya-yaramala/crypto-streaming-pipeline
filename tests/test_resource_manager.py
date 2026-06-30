from unittest.mock import MagicMock, patch

from consumer.resource_manager import (
    calculate_consumer_lag_estimate,
    check_resource_thresholds,
    get_system_resources,
)


def test_get_system_resources_structure():
    mock_mem = MagicMock()
    mock_mem.percent = 60.0
    mock_disk = MagicMock()
    mock_disk.percent = 40.0
    with patch("consumer.resource_manager.psutil.cpu_percent", return_value=50.0):
        with patch("consumer.resource_manager.psutil.virtual_memory", return_value=mock_mem):
            with patch("consumer.resource_manager.psutil.disk_usage", return_value=mock_disk):
                result = get_system_resources()
    assert "cpu_percent" in result
    assert isinstance(result["cpu_percent"], float)


def test_check_resource_thresholds_healthy():
    resources = {"cpu_percent": 50.0, "memory_percent": 60.0, "disk_percent": 40.0}
    result = check_resource_thresholds(resources)
    assert result["healthy"] is True
    assert result["warnings"] == []


def test_check_resource_thresholds_warning():
    resources = {"cpu_percent": 90.0, "memory_percent": 60.0, "disk_percent": 40.0}
    result = check_resource_thresholds(resources)
    assert result["healthy"] is False
    assert len(result["warnings"]) > 0


def test_calculate_consumer_lag_estimate_healthy():
    result = calculate_consumer_lag_estimate(1000, 990)
    assert result["status"] == "healthy"
    assert result["lag"] == 10


def test_calculate_consumer_lag_estimate_critical():
    result = calculate_consumer_lag_estimate(1000, 700)
    assert result["status"] == "critical"
    assert result["lag"] == 300

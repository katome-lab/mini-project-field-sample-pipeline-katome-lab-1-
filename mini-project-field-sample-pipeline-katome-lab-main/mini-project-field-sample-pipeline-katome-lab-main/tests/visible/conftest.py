"""
Pytest configuration for Mini-Project visible tests.
"""

import json
import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def variant_config():
    """Load student's variant configuration."""
    config_path = Path(__file__).parent.parent.parent / ".variant_config.json"
    if config_path.exists():
        with open(config_path) as f:
            return json.load(f)
    return {
        "parameters": {
            "study_area": "Northern Zone",
            "target_elements": ["Au", "Cu", "Pb"],
            "collector_filter": "A. Smith",
            "elevation_range": {"min": 1200, "max": 1600},
            "anomaly_percentile": 95,
            "num_samples": 800
        }
    }

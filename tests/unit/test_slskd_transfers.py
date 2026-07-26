from app.services.monitoring import map_slskd_transfer_state
from app.sources.base import CapabilityState


def test_slskd_state_mapping_downloaded() -> None:
    assert map_slskd_transfer_state(CapabilityState(True, "Completed")) == "downloaded"
    assert map_slskd_transfer_state(CapabilityState(True, "Completed, Succeeded")) == "downloaded"


def test_slskd_state_mapping_missing_is_failed() -> None:
    assert map_slskd_transfer_state(CapabilityState(False, "transfer not found")) == "failed"

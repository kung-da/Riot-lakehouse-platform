from pathlib import Path

from lakehouse.raw.detect_dataset import detect_dataset


def test_detect_match_payload():
    payload = {"metadata": {"matchId": "VN2_1"}, "info": {"participants": []}}
    assert detect_dataset(Path("raw/matches/VN2_1.json"), payload) == "matches"


def test_detect_timeline_payload():
    payload = {"metadata": {"matchId": "VN2_1"}, "info": {"frames": []}}
    assert detect_dataset(Path("raw/timelines/VN2_1.json"), payload) == "timelines"


def test_detect_ranked_payload():
    payload = {"entries": [], "tier": "MASTER"}
    assert detect_dataset(Path("raw/ranked/master.json"), payload) == "ranked"

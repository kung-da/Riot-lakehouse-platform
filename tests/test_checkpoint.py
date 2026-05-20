from pathlib import Path

from lakehouse.common.checkpoint import FileCheckpoint


def test_file_checkpoint_roundtrip(tmp_path: Path):
    checkpoint = FileCheckpoint(dataset="matches")
    checkpoint.mark_processed(Path("raw/matches/VN2_1.json"))
    checkpoint.save(tmp_path)

    loaded = FileCheckpoint.load(tmp_path, "matches")
    assert loaded.is_processed(Path("raw/matches/VN2_1.json"))

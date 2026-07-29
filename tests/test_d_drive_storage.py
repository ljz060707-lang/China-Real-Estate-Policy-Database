from policydb.settings import Settings
from policydb.storage import migrate_storage, verify_storage


def test_external_storage_migration_copies_and_keeps_source(tmp_path):
    root = tmp_path / "repo"
    source = root / "data/curated"
    source.mkdir(parents=True)
    (source / "x.bin").write_bytes(b"immutable")
    target = tmp_path / "CRPD"
    result = migrate_storage(Settings(root=root), target=target, confirm=True)
    assert result["copied"] == 1
    assert (source / "x.bin").read_bytes() == b"immutable"
    assert verify_storage(Settings(root=root), target=target)["passed"]

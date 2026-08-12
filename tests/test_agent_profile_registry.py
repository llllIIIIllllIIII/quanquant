import json
import threading

import pytest

from quanquant.agent import profile_registry as pr


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    monkeypatch.setattr(pr, "REGISTRY_PATH", tmp_path / "profiles.json")
    monkeypatch.setattr(pr, "LOCK_PATH", tmp_path / "profiles.lock")
    return tmp_path


def test_upsert_then_list_roundtrip():
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="alice",
                       buffer_path="/x/outbox.db")
    entries = pr.list_profiles(site_origin="https://q.example")
    assert len(entries) == 1 and entries[0].profile_id == "1" and entries[0].username == "alice"


def test_upsert_same_profile_id_updates_not_duplicates():
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="alice", buffer_path="/x")
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="alice2", buffer_path="/x")
    entries = pr.list_profiles(site_origin="https://q.example")
    assert len(entries) == 1 and entries[0].username == "alice2"


def test_remove_profile_deletes_entry():
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x")
    pr.remove_profile(site_origin="https://q.example", profile_id="1")
    assert pr.list_profiles(site_origin="https://q.example") == []


def test_registry_file_is_valid_json_after_write(_isolated_home):
    pr.upsert_profile(site_origin="https://q.example", profile_id="1", username="a", buffer_path="/x")
    data = json.loads((_isolated_home / "profiles.json").read_text())
    assert "https://q.example" in data


def test_buffer_path_for_is_fixed_length_hex_and_stable():
    p1 = pr.buffer_path_for(site_origin="https://q.example:443", profile_id="1")
    p2 = pr.buffer_path_for(site_origin="https://q.example:443", profile_id="1")
    assert p1 == p2
    assert len(p1.parent.name) == 16 and all(c in "0123456789abcdef" for c in p1.parent.name)


def test_buffer_path_differs_by_port_even_with_same_host():
    p_http = pr.buffer_path_for(site_origin="http://q.example:8000", profile_id="1")
    p_other = pr.buffer_path_for(site_origin="http://q.example:9000", profile_id="1")
    assert p_http != p_other  # 同 host 異 port 不共用 outbox（spec §5.3 BLOCKER 修復）


def test_concurrent_upserts_from_multiple_threads_do_not_corrupt_registry(_isolated_home):
    def _worker(i):
        pr.upsert_profile(site_origin="https://q.example", profile_id=str(i),
                           username=f"user{i}", buffer_path=f"/x{i}")
    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()
    data = json.loads((_isolated_home / "profiles.json").read_text())
    assert len(data["https://q.example"]) == 20  # 無互蓋、無遺失


def test_instance_lock_second_acquire_raises():
    lock_path = pr.buffer_path_for(site_origin="https://q.example", profile_id="1")
    lock1 = pr.InstanceLock(lock_path)
    lock1.acquire()
    try:
        lock2 = pr.InstanceLock(lock_path)
        with pytest.raises(pr.AgentAlreadyRunningError):
            lock2.acquire()
    finally:
        lock1.release()

"""#163 — the SQLite trigger store shares one ``sqlite3`` connection across threads.

The async methods serialize through an asyncio lock, but ``list_sync`` is also
called straight from the synchronous router build, from other threads. Two
threads driving one connection at once crashed the CI process (a segfault in
``list_sync``), so every use of the connection holds the store's thread lock.
Without it this test can take the interpreter down rather than fail an assert.
"""
from __future__ import annotations

import threading

from cairn.core.storage.trigger_store import SqliteTriggerStore, StoredTrigger


def _row(i: int) -> StoredTrigger:
    return StoredTrigger(id=f"t{i}", name=f"t{i}", source="custom", yaml=f"name: t{i}\n")


def test_concurrent_reads_and_writes_on_one_connection_do_not_crash(tmp_path):
    store = SqliteTriggerStore(path=str(tmp_path / "triggers.db"))
    errors: list[BaseException] = []
    stop = threading.Event()

    def reader() -> None:
        try:
            while not stop.is_set():
                store.list_sync()
                store.list_sync("default")
        except BaseException as e:  # noqa: BLE001 — collected and asserted below
            errors.append(e)

    def writer(start: int) -> None:
        try:
            for i in range(start, start + 150):
                store._create_sync(_row(i))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    readers = [threading.Thread(target=reader) for _ in range(4)]
    writers = [threading.Thread(target=writer, args=(n * 1000,)) for n in range(3)]
    for t in readers + writers:
        t.start()
    for t in writers:
        t.join(timeout=60)
    stop.set()
    for t in readers:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in readers + writers), "a thread hung on the store"

    assert errors == []
    assert len(store.list_sync()) == 450
    store.close()


def test_close_waits_for_an_in_flight_read(tmp_path):
    """``close`` holds the same lock, so it cannot pull the connection out from
    under a read on another thread. (On a tree without the lock this fails with
    AttributeError, so the close race itself is proven by the test above.)"""
    store = SqliteTriggerStore(path=str(tmp_path / "triggers.db"))
    for i in range(50):
        store._create_sync(_row(i))
    with store._conn_lock:
        closer = threading.Thread(target=store.close)
        closer.start()
        closer.join(timeout=0.2)
        assert closer.is_alive(), "close() ran while another thread held the connection"
        assert len(store.list_sync()) == 50  # the connection is still usable here
    closer.join(timeout=5)
    assert not closer.is_alive()

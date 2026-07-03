"""
kafka_to_adls_bridge.py
=======================
Phase 5 — LedgerFlow Lakehouse

Reads CDC events from local Kafka (Debezium output) and writes them as
partitioned JSON files to Azure Data Lake Storage Gen2 (raw/ container).

Path pattern:
  raw/{table}/year=YYYY/month=MM/day=DD/hour=HH/events_{ts}.json

Each file is a newline-delimited JSON (NDJSON) batch — one event per line.
Databricks Auto Loader (Phase 6) will pick these up automatically.

Run:
  python scripts/kafka_to_adls_bridge.py

Stop with Ctrl+C — it flushes any pending batch before exiting.
"""

import json
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from azure.storage.filedatalake import DataLakeServiceClient
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv

# ─── Config ───────────────────────────────────────────────────────────────────

load_dotenv()

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ADLS_ACCOUNT_NAME = os.getenv("ADLS_ACCOUNT_NAME")
ADLS_ACCOUNT_KEY  = os.getenv("ADLS_ACCOUNT_KEY")
RAW_CONTAINER     = os.getenv("ADLS_CONTAINER", "raw")

TOPICS = [
    "ledgerflow.public.customers",
    "ledgerflow.public.loans",
    "ledgerflow.public.transactions",
]

# Table name extracted from topic (last segment)
TOPIC_TO_TABLE = {t: t.split(".")[-1] for t in TOPICS}

# Batch settings
BATCH_SIZE     = 50        # flush after this many events per table
FLUSH_INTERVAL = 30        # flush every N seconds even if batch not full

# ─── ADLS Client ──────────────────────────────────────────────────────────────

def get_adls_client() -> DataLakeServiceClient:
    """Create ADLS Gen2 client using storage account key."""
    if not ADLS_ACCOUNT_NAME or not ADLS_ACCOUNT_KEY:
        print("[ERROR] ADLS_ACCOUNT_NAME or ADLS_ACCOUNT_KEY missing in .env")
        sys.exit(1)
    account_url = f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net"
    return DataLakeServiceClient(
        account_url=account_url,
        credential=ADLS_ACCOUNT_KEY,
    )


def upload_batch(client: DataLakeServiceClient, table: str, events: list):
    """Write a batch of events as NDJSON to ADLS Gen2 raw container."""
    now = datetime.now(timezone.utc)
    path = (
        f"{table}/"
        f"year={now.strftime('%Y')}/"
        f"month={now.strftime('%m')}/"
        f"day={now.strftime('%d')}/"
        f"hour={now.strftime('%H')}"
    )
    filename = f"events_{now.strftime('%Y%m%d_%H%M%S_%f')}.json"

    # Newline-delimited JSON
    ndjson_content = "\n".join(json.dumps(e) for e in events) + "\n"
    data = ndjson_content.encode("utf-8")

    try:
        fs_client   = client.get_file_system_client(RAW_CONTAINER)
        dir_client  = fs_client.get_directory_client(path)
        dir_client.create_directory()                     # no-op if exists
        file_client = dir_client.get_file_client(filename)
        file_client.upload_data(data, overwrite=True)
        print(
            f"  [ADLS] ✓ raw/{path}/{filename}  "
            f"({len(events)} events, {len(data):,} bytes)"
        )
    except Exception as exc:
        print(f"  [ADLS] ✗ Upload failed for {table}: {exc}")
        raise


# ─── Batch manager ────────────────────────────────────────────────────────────

class BatchManager:
    def __init__(self):
        self.buffers = defaultdict(list)
        self.last_flush = time.time()

    def add(self, table: str, event: dict):
        self.buffers[table].append(event)

    def should_flush(self, table: str) -> bool:
        return len(self.buffers[table]) >= BATCH_SIZE

    def time_to_flush(self) -> bool:
        return (time.time() - self.last_flush) >= FLUSH_INTERVAL

    def flush(self, client: DataLakeServiceClient, table: str = None):
        """Flush one table's buffer, or all tables if table=None."""
        tables = [table] if table else list(self.buffers.keys())
        for tbl in tables:
            if self.buffers[tbl]:
                upload_batch(client, tbl, self.buffers[tbl])
                self.buffers[tbl] = []
        self.last_flush = time.time()

    def flush_all(self, client: DataLakeServiceClient):
        self.flush(client, table=None)


# ─── Kafka Consumer ───────────────────────────────────────────────────────────

def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           "ledgerflow-adls-bridge",
        "auto.offset.reset":  "earliest",          # catch historical events
        "enable.auto.commit": False,               # manual commit after upload
        "max.poll.interval.ms": 300000,
    })


# ─── Graceful shutdown ────────────────────────────────────────────────────────

_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    print("\n[BRIDGE] Shutdown signal received — flushing remaining events...")
    _shutdown = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ─── Main loop ────────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  LedgerFlow — Kafka -> ADLS Gen2 Bridge")
    print(f"  Kafka  : {KAFKA_BOOTSTRAP}")
    print(f"  ADLS   : {ADLS_ACCOUNT_NAME}.dfs.core.windows.net / {RAW_CONTAINER}")
    print(f"  Topics : {', '.join(TOPICS)}")
    print(f"  Batch  : {BATCH_SIZE} events or {FLUSH_INTERVAL}s (whichever first)")
    print("=" * 65)

    adls     = get_adls_client()
    batch    = BatchManager()
    consumer = build_consumer()
    consumer.subscribe(TOPICS)

    total_events = 0
    total_files  = 0

    try:
        print("[BRIDGE] Listening for CDC events...\n")
        while not _shutdown:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message — check time-based flush
                if batch.time_to_flush():
                    has_pending = any(batch.buffers.values())
                    if has_pending:
                        n_before = sum(len(v) for v in batch.buffers.values())
                        print(f"[BRIDGE] {FLUSH_INTERVAL}s interval — flushing all tables ({n_before} events)")
                        batch.flush_all(adls)
                        consumer.commit()
                        total_files += 1
                        print()
                    else:
                        batch.last_flush = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # Parse event
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue  # skip malformed messages

            topic = msg.topic()
            table = TOPIC_TO_TABLE.get(topic, topic.split(".")[-1])

            # Enrich with bridge metadata
            event["_bridge_ts"]    = datetime.now(timezone.utc).isoformat()
            event["_kafka_offset"] = msg.offset()
            event["_topic"]        = topic

            batch.add(table, event)
            total_events += 1

            op = event.get("__op", event.get("op", "r"))
            print(
                f"  [{table.upper()[:5]:5s}] "
                f"op={op}  offset={msg.offset()}  "
                f"buf={len(batch.buffers[table])}/{BATCH_SIZE}"
            )

            # Flush if batch is full
            if batch.should_flush(table):
                print(f"[BRIDGE] Batch full for {table} — uploading...")
                batch.flush(adls, table)
                consumer.commit()
                total_files += 1
                print()

    finally:
        # Flush everything on exit
        has_pending = any(batch.buffers.values())
        if has_pending:
            n = sum(len(v) for v in batch.buffers.values())
            print(f"[BRIDGE] Flushing {n} remaining events before exit...")
            batch.flush_all(adls)
            consumer.commit()
            total_files += 1
        consumer.close()

    print("\n" + "=" * 65)
    print(f"  Bridge stopped.")
    print(f"  Total events processed : {total_events}")
    print(f"  Total files uploaded   : {total_files}")
    print("=" * 65)


if __name__ == "__main__":
    main()

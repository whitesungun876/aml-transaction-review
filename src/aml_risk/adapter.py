"""Strict positional mapping: IBM CSV deliberately repeats the Account header."""
import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .common import digest, file_sha, write_json

HEADER = ["Timestamp", "From Bank", "Account", "To Bank", "Account", "Amount Received", "Receiving Currency", "Amount Paid", "Payment Currency", "Payment Format", "Is Laundering"]
SCHEMA = pa.schema([
    ("transaction_id", pa.string()), ("dataset_version", pa.string()),
    ("event_time", pa.timestamp("us")), ("raw_time", pa.string()),
    ("sender_bank", pa.string()), ("sender_account", pa.string()),
    ("receiver_bank", pa.string()), ("receiver_account", pa.string()),
    ("received_amount", pa.decimal128(30, 8)), ("received_currency", pa.string()),
    ("payment_amount", pa.decimal128(30, 8)), ("payment_currency", pa.string()),
    ("payment_format", pa.string()), ("source_file_sha256", pa.string()),
    ("source_record_number", pa.int64()),
])
LABEL_SCHEMA = pa.schema([("transaction_id", pa.string()), ("label", pa.int8())])


def parse_row(row, record, sha, version):
    if len(row) != len(HEADER):
        raise ValueError("column_count")
    if any(not x.strip() for x in row):
        raise ValueError("missing_field")
    try:
        time = datetime.strptime(row[0], "%Y/%m/%d %H:%M")
    except ValueError as exc:
        raise ValueError("invalid_time") from exc
    amounts = []
    for index in (5, 7):
        try:
            amount = Decimal(row[index])
        except InvalidOperation as exc:
            raise ValueError("invalid_amount") from exc
        if not amount.is_finite() or amount < 0 or amount >= Decimal("1e22") or amount.as_tuple().exponent < -8:
            raise ValueError("invalid_amount")
        amounts.append(amount)
    if row[10] not in ("0", "1"):
        raise ValueError("invalid_label")
    tid = f"{sha}:{record}"
    transaction = dict(zip(SCHEMA.names, [tid, version, time, row[0], row[1], row[2], row[3], row[4], amounts[0], row[6], amounts[1], row[8], row[9], sha, record]))
    return transaction, {"transaction_id": tid, "label": int(row[10])}


def prepare(source, manifest, output, config):
    source, output = Path(source), Path(output)
    if output.exists():
        raise FileExistsError("output exists; preserve prior run")
    if file_sha(source) != manifest["sha256"]:
        raise ValueError("source SHA-256 mismatch")
    output.mkdir(parents=True)
    version = digest({"dataset": manifest["dataset"], "release": manifest["release"], "sha256": manifest["sha256"], "mapping": "ibm-positional-v1"})
    start = datetime.fromisoformat(config["observation_start"])
    end = datetime.fromisoformat(config["observation_end_exclusive"])
    counts = dict(input=0, accepted=0, quarantined=0, in_scope=0, out_of_scope=0, positive_in_scope=0, positive_out_of_scope=0)
    errors = {}
    transactions, labels = [], []
    minimum, maximum = None, None
    with source.open(newline="", encoding="utf-8-sig") as stream, pq.ParquetWriter(output / "transactions.parquet", SCHEMA) as tw, pq.ParquetWriter(output / "labels.parquet", LABEL_SCHEMA) as lw, (output / "quarantine.jsonl").open("w") as quarantine:
        reader = csv.reader(stream)
        if next(reader, None) != HEADER:
            raise ValueError("unexpected header; duplicate Account columns require positional mapping")
        for record, row in enumerate(reader, start=1):
            counts["input"] += 1
            try:
                transaction, label = parse_row(row, record, manifest["sha256"], version)
            except ValueError as exc:
                counts["quarantined"] += 1
                code = str(exc)
                errors[code] = errors.get(code, 0) + 1
                quarantine.write(json.dumps({"source_record_number": record, "error": code}) + "\n")
                continue
            counts["accepted"] += 1
            time = transaction["event_time"]
            minimum = time if minimum is None else min(time, minimum)
            maximum = time if maximum is None else max(time, maximum)
            if not start <= time < end:
                counts["out_of_scope"] += 1
                counts["positive_out_of_scope"] += label["label"]
                continue
            counts["in_scope"] += 1
            counts["positive_in_scope"] += label["label"]
            transactions.append(transaction)
            labels.append(label)
            if len(transactions) >= 25000:
                tw.write_table(pa.Table.from_pylist(transactions, schema=SCHEMA))
                lw.write_table(pa.Table.from_pylist(labels, schema=LABEL_SCHEMA))
                transactions.clear(); labels.clear()
        if transactions:
            tw.write_table(pa.Table.from_pylist(transactions, schema=SCHEMA))
            lw.write_table(pa.Table.from_pylist(labels, schema=LABEL_SCHEMA))
    report = {"counts": counts, "errors": errors, "dataset_version": version,
              "min_time": str(minimum), "max_time": str(maximum), "source": manifest,
              "source_record_number_semantics": "1-based CSV logical record excluding header; NOT physical line number",
              "protocol_sha256": digest(config)}
    write_json(output / "quality_report.json", report)
    assert counts["input"] == counts["accepted"] + counts["quarantined"]
    assert counts["accepted"] == counts["in_scope"] + counts["out_of_scope"]
    if counts["quarantined"] or not counts["in_scope"]:
        raise ValueError("strict quality gate failed; no successful completion marker")
    write_json(output / "COMPLETE.json", {"quality_sha256": file_sha(output / "quality_report.json"), "transactions_sha256": file_sha(output / "transactions.parquet"), "labels_sha256": file_sha(output / "labels.parquet")})
    return report

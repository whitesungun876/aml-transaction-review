"""DuckDB time-range features; ties excluded by a strict microsecond bound."""
from pathlib import Path
import duckdb
from .common import digest, file_sha, write_json

NUMERIC = ["log_payment_amount", "cross_bank", "send_count_1h", "send_count_24h", "counterparties_24h", "same_currency_count_24h", "log_mean_amount_24h", "amount_to_history_ratio", "new_counterparty", "cold_start", "history_age_hours", "currency_history_missing"]
CATEGORICAL = ["payment_currency", "received_currency", "payment_format"]
FEATURES = NUMERIC + CATEGORICAL
META = ["transaction_id", "dataset_version", "event_time", "sender_bank", "sender_account", "receiver_bank", "receiver_account", "source_file_sha256", "source_record_number"]
SIGNATURE = {"version": "aml-feature-v1", "numeric": NUMERIC, "categorical": CATEGORICAL, "windows": [3600, 86400], "history_interval": "[t-window,t)", "timezone": "unspecified", "missing": "zero plus explicit indicators", "account_id_as_feature": False}


def sql_features():
    return """
    WITH h AS (
      SELECT *,
        count(*) OVER w1 AS send_count_1h,
        count(*) OVER w24 AS send_count_24h,
        count(DISTINCT (receiver_bank,receiver_account)) OVER w24 AS counterparties_24h,
        count(*) OVER wc AS same_currency_count_24h,
        avg(payment_amount) OVER wc AS mean_amount,
        min(event_time) OVER wa AS first_seen,
        min(event_time) OVER (PARTITION BY sender_bank,sender_account,receiver_bank,receiver_account) AS pair_first
      FROM tx
      WINDOW
        w1 AS (PARTITION BY sender_bank,sender_account ORDER BY event_time RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING),
        w24 AS (PARTITION BY sender_bank,sender_account ORDER BY event_time RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING),
        wc AS (PARTITION BY sender_bank,sender_account,payment_currency ORDER BY event_time RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING),
        wa AS (PARTITION BY sender_bank,sender_account ORDER BY event_time RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 1 MICROSECOND PRECEDING)
    ) SELECT transaction_id,dataset_version,event_time,sender_bank,sender_account,receiver_bank,receiver_account,source_file_sha256,source_record_number,
        ln(1+cast(payment_amount AS DOUBLE)) AS log_payment_amount,
        cast(sender_bank<>receiver_bank AS INTEGER) AS cross_bank,
        send_count_1h,send_count_24h,counterparties_24h,same_currency_count_24h,
        ln(1+coalesce(mean_amount,0)) AS log_mean_amount_24h,
        CASE WHEN mean_amount>0 THEN cast(payment_amount AS DOUBLE)/mean_amount ELSE 0 END AS amount_to_history_ratio,
        cast(pair_first=event_time AS INTEGER) AS new_counterparty,
        cast(first_seen IS NULL AS INTEGER) AS cold_start,
        coalesce(epoch(event_time-first_seen)/3600,0) AS history_age_hours,
        cast(mean_amount IS NULL OR mean_amount=0 AS INTEGER) AS currency_history_missing,
        payment_currency,received_currency,payment_format
    FROM h ORDER BY event_time,transaction_id
    """


def build(transactions, output, config):
    if config.get("windows_seconds", [3600,86400]) != SIGNATURE["windows"]:
        raise ValueError("configured windows differ from implemented feature signature")
    output = Path(output)
    if output.exists():
        raise FileExistsError("features output exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=?", [config.get("threads", 2)])
    con.execute("SET memory_limit=?", [config.get("duckdb_memory_limit", "3GB")])
    temp = output.parent / "duckdb_tmp"
    con.execute("SET temp_directory=?", [str(temp)])
    con.from_parquet(str(transactions)).create_view("tx")
    partial = output.with_suffix(".partial.parquet")
    con.sql(sql_features()).write_parquet(str(partial))
    count = con.read_parquet(str(partial)).count("*").fetchone()[0]
    partial.rename(output)
    manifest = {"signature": SIGNATURE, "feature_version": digest(SIGNATURE), "input_sha256": file_sha(transactions), "output_sha256": file_sha(output), "rows": count}
    write_json(output.with_suffix(".manifest.json"), manifest)
    con.close()
    return manifest

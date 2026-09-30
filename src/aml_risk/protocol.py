import hashlib
from pathlib import Path
import duckdb
from .common import digest, file_sha, write_json
from .features import SIGNATURE


def freeze(features, config, output):
    output = Path(output)
    if output.exists():
        raise FileExistsError("split manifest already frozen")
    times = [config[k] for k in ("observation_start", "training_start", "validation_start", "test_start", "observation_end_exclusive")]
    from datetime import datetime
    parsed = [datetime.fromisoformat(t) for t in times]
    if not all(a < b for a, b in zip(parsed, parsed[1:])):
        raise ValueError("time boundaries must strictly increase")
    con = duckdb.connect()
    con.from_parquet(str(features)).create_view("f")
    total, unique = con.execute("SELECT count(*),count(DISTINCT transaction_id) FROM f").fetchone()
    if total != unique or not total:
        raise ValueError("empty features or duplicate transaction ID")
    if con.execute("SELECT count(*) FROM f WHERE event_time<? OR event_time>=?", [times[0], times[-1]]).fetchone()[0]:
        raise ValueError("features outside observation range")
    result = {"config": config, "protocol_sha256": digest(config), "feature_signature": SIGNATURE,
              "features_sha256": file_sha(features), "splits": {}}
    for i, name in enumerate(("warmup", "train", "validation", "test")):
        rows = con.execute("SELECT transaction_id FROM f WHERE event_time>=? AND event_time<? ORDER BY transaction_id", times[i:i+2]).fetchall()
        if not rows:
            raise ValueError(f"empty split: {name}")
        sha = hashlib.sha256()
        for (tid,) in rows:
            sha.update((tid+"\n").encode())
        result["splits"][name] = {"start": times[i], "end_exclusive": times[i+1], "rows": len(rows), "ids_sha256": sha.hexdigest()}
    if sum(x["rows"] for x in result["splits"].values()) != total:
        raise ValueError("split row reconciliation failed")
    write_json(output, result)
    con.close()
    return result

import argparse
import json
from pathlib import Path
from aml_risk.adapter import prepare
from aml_risk.features import build

p = argparse.ArgumentParser()
p.add_argument("--source", type=Path, required=True)
p.add_argument("--manifest", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
p.add_argument("--config", type=Path, default=Path("configs/aml_experiment.json"))
a = p.parse_args()
c = json.loads(a.config.read_text())
r = prepare(a.source, json.loads(a.manifest.read_text()), a.output, c)
print(json.dumps({"adapter": r["counts"]}), flush=True)
print(json.dumps({"features": build(a.output / "transactions.parquet", a.output / "features.parquet", c)}), flush=True)

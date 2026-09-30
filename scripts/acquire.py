"""Fetch a single public IBM file; no credentials, no bulk dataset download."""
import hashlib
import json
import shutil
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REF = "ealtman2019/ibm-transactions-for-anti-money-laundering-aml"


def sha(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def main():
    target = ROOT / ".runtime" / "raw"
    target.mkdir(parents=True, exist_ok=True)
    metadata_url = f"https://www.kaggle.com/api/v1/datasets/view/{REF}"
    with urllib.request.urlopen(metadata_url, timeout=60) as response:
        metadata = json.load(response)
    if metadata["currentVersionNumber"] != 8:
        raise RuntimeError("dataset release changed; review protocol before acquisition")
    metadata_path = target / "provider_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2))
    file = target / "HI-Small_Trans.csv"
    if not file.exists():
        url = f"https://www.kaggle.com/api/v1/datasets/download/{REF}/HI-Small_Trans.csv?datasetVersionNumber=8"
        download = target / "download.part"
        with urllib.request.urlopen(url, timeout=120) as response, download.open("wb") as output:
            shutil.copyfileobj(response, output)
        if zipfile.is_zipfile(download):
            with zipfile.ZipFile(download) as archive:
                member = next(x for x in archive.namelist() if x == "HI-Small_Trans.csv")
                with archive.open(member) as source, file.open("wb") as output:
                    shutil.copyfileobj(source, output)
        else:
            download.rename(file)
    # Some gateways end large responses early. Resume only with a validated
    # Content-Range; never treat a partial CSV as the complete dataset.
    expected_size = 475664283
    while file.stat().st_size < expected_size:
        offset = file.stat().st_size
        end = min(offset + 8_000_000, expected_size) - 1
        url = f"https://www.kaggle.com/api/v1/datasets/download/{REF}/HI-Small_Trans.csv?datasetVersionNumber=8"
        request = urllib.request.Request(url, headers={"Range": f"bytes={offset}-{end}"})
        with urllib.request.urlopen(request, timeout=120) as response:
            if response.status != 206 or response.headers.get("Content-Range") != f"bytes {offset}-{end}/{expected_size}":
                raise RuntimeError("range resume not validated")
            payload = response.read()
        if len(payload) != end - offset + 1:
            raise RuntimeError("incomplete range; preserve partial and retry acquisition")
        with file.open("ab") as output:
            output.write(payload)
        print(json.dumps({"downloaded_bytes": file.stat().st_size, "expected_bytes": expected_size}), flush=True)
    if file.stat().st_size != expected_size:
        raise RuntimeError("unexpected source size; no unreviewed data accepted")
    manifest = {
        "dataset": REF, "release": 8, "source_url": metadata_url,
        "file": file.name, "size": file.stat().st_size, "sha256": sha(file),
        "license": metadata.get("licenseName"),
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "provider_metadata_sha256": sha(metadata_path),
        "time_semantics": "naive timestamps, provider does not declare timezone",
    }
    (target / "source_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest))


if __name__ == "__main__":
    main()

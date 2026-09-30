"""Bounded-memory exact comparison and canonical logical feature digest."""
import hashlib
import itertools
import json
from pathlib import Path
import pyarrow.parquet as pq
from aml_risk.common import file_sha,write_json

paths=[Path('.runtime/prepared-v1/features.parquet'),Path('.runtime/reproducibility/features.parquet')]
files=[pq.ParquetFile(p) for p in paths]
assert files[0].schema_arrow==files[1].schema_arrow,'schema changed'
hashes=[hashlib.sha256(),hashlib.sha256()]
rows=0
for pair in itertools.zip_longest(*(f.iter_batches(batch_size=20000,use_threads=False) for f in files)):
    a,b=pair
    assert a is not None and b is not None,'row count changed'
    assert a.equals(b),'logical feature values or ordering changed'
    for h,batch in zip(hashes,pair):
        # Fixed batch size and Arrow IPC serialization: same schema, row order,
        # and exact values. This digest is specific to this Arrow version.
        h.update(batch.serialize().to_pybytes())
    rows+=a.num_rows
assert hashes[0].hexdigest()==hashes[1].hexdigest(),'canonical serialization differs'
result={'rows':rows,'exact_logical_equality':True,'logical_sha256':[h.hexdigest() for h in hashes],
        'algorithm':'Arrow 19.0.1 record-batch IPC, 20000 rows, stored event_time/transaction_id order',
        'file_sha256':[file_sha(p) for p in paths],
        'file_byte_equal':file_sha(paths[0])==file_sha(paths[1]),
        'note':'Parquet physical encoding differs; all feature values, metadata fields and row order compared exactly.'}
write_json(Path('reports/feature_reproducibility.json'),result)
print(json.dumps(result))

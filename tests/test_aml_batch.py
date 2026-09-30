import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import numpy as np
import pandas as pd
from aml_risk.features import FEATURES,META,SIGNATURE
from aml_risk.model import Ranker,fit_rules
from aml_risk.batch import publish
from test_aml_model import sample


class BatchTests(unittest.TestCase):
    def data(self):
        f=sample()
        for name in META: f[name]="fixture"
        f["transaction_id"]=[f"t{i}" for i in range(len(f))]
        f["event_time"]=pd.date_range("2022-09-09",periods=len(f),freq="h")
        f["source_record_number"]=np.arange(len(f))+1
        return f[META+FEATURES]

    def test_publish_replay_conflict_and_tamper(self):
        f=self.data(); model=Ranker.fit(f[FEATURES],np.arange(len(f))%3==0,{"n_estimators":3},1)
        with tempfile.TemporaryDirectory() as root:
            root=Path(root); model.save(root/"model",{})
            identity={"batch_id":"b1","feature_signature":SIGNATURE}
            rules=fit_rules(f[FEATURES])
            first=publish(f,root/"model",identity,root/"out",rules)
            second=publish(f,root/"model",identity,root/"out",rules)
            self.assertEqual(second["status"],"verified_replay")
            self.assertEqual(first["rows"],40)
            changed=f.copy(); changed.loc[0,"cross_bank"]+=1
            with self.assertRaisesRegex(ValueError,"conflict"): publish(changed,root/"model",identity,root/"out",rules)
            with self.assertRaisesRegex(ValueError,"schema"): publish(f.assign(label=0),root/"model",identity,root/"out",rules)
            with open(Path(first["path"])/"scores.parquet","ab") as stream: stream.write(b"tamper")
            with self.assertRaisesRegex(ValueError,"checksum"): publish(f,root/"model",identity,root/"out",rules)

    def test_failed_write_leaves_no_completed_version_and_retries(self):
        f=self.data(); model=Ranker.fit(f[FEATURES],np.arange(len(f))%3==0,{"n_estimators":2},1)
        with tempfile.TemporaryDirectory() as root:
            root=Path(root); manifest=model.save(root/"model",{})
            identity={"batch_id":"b1","feature_signature":SIGNATURE}; rules=fit_rules(f[FEATURES])
            with patch("pandas.DataFrame.to_parquet",side_effect=OSError("injected write failure")):
                with self.assertRaises(OSError): publish(f,root/"model",identity,root/"out",rules)
            self.assertFalse((root/"out"/"b1"/manifest["model_version"]).exists())
            self.assertEqual(publish(f,root/"model",identity,root/"out",rules)["status"],"published")


if __name__=="__main__": unittest.main()

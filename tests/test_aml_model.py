import tempfile
import unittest
from pathlib import Path
import numpy as np
import pandas as pd
from aml_risk.model import Ranker,fit_rules,score_rules,validate
from aml_risk.features import FEATURES,NUMERIC,CATEGORICAL
from aml_risk.evaluation import evaluate


def sample():
    frame=pd.DataFrame({name:np.arange(40,dtype=float)%7 for name in NUMERIC})
    for name in CATEGORICAL: frame[name]=["A","B"]*20
    return frame[FEATURES]


class ModelTests(unittest.TestCase):
    def test_roundtrip_unknown_categories_and_integrity(self):
        frame=sample(); labels=np.arange(40)%3==0
        ranker=Ranker.fit(frame,labels,{"n_estimators":4,"max_depth":2},7)
        test=frame.copy(); test.loc[0,CATEGORICAL]="UNSEEN"
        original=ranker.score(test)
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/"model"; ranker.save(path,{"fixture":True})
            loaded=Ranker.load(path)
            np.testing.assert_allclose(loaded.score(test),original,atol=1e-6)
            with (path/"model.json").open("a") as stream: stream.write(" ")
            with self.assertRaisesRegex(ValueError,"checksum"): Ranker.load(path)

    def test_signature_label_and_order_rejection(self):
        frame=sample()
        for wrong in [frame.assign(label=0),frame[FEATURES[::-1]],frame.assign(log_payment_amount=np.inf)]:
            with self.assertRaises(ValueError): validate(wrong)
        with self.assertRaises(ValueError): Ranker.fit(frame,np.zeros(40),{},7)

    def test_rules_freeze_and_evaluation_zero_positive(self):
        frame=sample(); thresholds=fit_rules(frame)
        before=thresholds.copy(); score_rules(frame,thresholds)
        self.assertEqual(before,thresholds)
        metadata=pd.DataFrame({"transaction_id":[str(i) for i in range(40)],"event_time":pd.date_range("2022-09-01",periods=40,freq="h")})
        result=evaluate(metadata,score_rules(frame,thresholds),np.zeros(40))
        self.assertIsNone(result["overall"][0]["recall_at_k"])
        self.assertIsNone(result["overall"][0]["average_precision"])
        self.assertEqual(result["daily_micro"][0]["reviewed"],2)


if __name__=="__main__": unittest.main()

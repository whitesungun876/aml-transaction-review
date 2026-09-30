import csv
import io
import json
import math
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from aml_risk.adapter import HEADER, SCHEMA, parse_row, prepare
from aml_risk.common import file_sha
from aml_risk.features import sql_features, NUMERIC, CATEGORICAL
from aml_risk.protocol import freeze


def raw(time="2022/09/01 00:00", amount="10", sender_bank="01", receiver="B", currency="USD", label="0"):
    return [time, sender_bank, "A", "02", receiver, amount, currency, amount, currency, "Cash", label]


def transaction(row, record=1):
    return parse_row(row, record, "sha", "v1")[0]


def calculate(rows):
    con = duckdb.connect()
    con.register("tx", pa.Table.from_pylist(rows, schema=SCHEMA))
    result = con.sql(sql_features()).fetchdf().set_index("transaction_id").to_dict("index")
    con.close()
    return result


class AdapterTests(unittest.TestCase):
    def test_duplicate_headers_map_accounts_positionally(self):
        value, label = parse_row(raw(receiver="different", label="1"), 2, "sha", "v")
        self.assertEqual(value["sender_account"], "A")
        self.assertEqual(value["receiver_account"], "different")
        self.assertNotIn("label", value)
        self.assertEqual(label["label"], 1)
        self.assertEqual(value["transaction_id"], "sha:2")
        self.assertIsNone(value["event_time"].tzinfo)

    def test_invalid_fields_fail(self):
        for field, value in [(0,"bad"),(5,"NaN"),(7,"-1"),(7,"Infinity"),(7,"1e30"),(10,"2"),(1,"")]:
            with self.subTest(field=field, value=value):
                row = raw(); row[field] = value
                with self.assertRaises(ValueError): parse_row(row, 1, "s", "v")
        with self.assertRaises(ValueError): parse_row(raw()[:-1], 1, "s", "v")

    def test_strict_reconciliation_and_no_completion_marker(self):
        with tempfile.TemporaryDirectory() as root:
            source = Path(root)/"data.csv"
            with source.open("w", newline="") as handle:
                writer = csv.writer(handle); writer.writerow(HEADER)
                writer.writerows([raw(), raw(amount="NaN"), raw(time="2022/09/12 00:00")])
            manifest = {"sha256":file_sha(source),"dataset":"test","release":1}
            output=Path(root)/"out"
            with self.assertRaisesRegex(ValueError,"strict quality"):
                prepare(source,manifest,output,{"observation_start":"2022-09-01", "observation_end_exclusive":"2022-09-11"})
            counts=json.loads((output/"quality_report.json").read_text())["counts"]
            self.assertEqual((counts["input"],counts["accepted"],counts["quarantined"],counts["out_of_scope"]),(3,2,1,1))
            self.assertFalse((output/"COMPLETE.json").exists())
            with self.assertRaises(FileExistsError): prepare(source,manifest,output,{})

    def test_sha_header_and_duplicate_content(self):
        with tempfile.TemporaryDirectory() as root:
            source=Path(root)/"data.csv"
            stream=io.StringIO(); writer=csv.writer(stream); writer.writerow(HEADER); writer.writerows([raw(),raw()])
            source.write_text(stream.getvalue())
            output=Path(root)/"out"
            with self.assertRaisesRegex(ValueError,"SHA"):
                prepare(source,{"sha256":"wrong"},output,{})
            report=prepare(source,{"sha256":file_sha(source),"dataset":"test","release":1},output,{"observation_start":"2022-09-01","observation_end_exclusive":"2022-09-11"})
            self.assertEqual(report["counts"]["accepted"],2)
            table=pq.read_table(output/"transactions.parquet")
            self.assertEqual(len(set(table["transaction_id"].to_pylist())),2)


class FeatureTests(unittest.TestCase):
    def test_independent_reference_all_features(self):
        rows=self.sample()+[
            transaction(raw(time="2022/09/02 01:01", amount="0", receiver="B"),5),
            transaction(raw(time="2022/09/03 00:00", amount="90", sender_bank="77"),6),
        ]
        actual=calculate(rows)
        for row in rows:
            past=[r for r in rows if (r["sender_bank"],r["sender_account"])==(row["sender_bank"],row["sender_account"]) and r["event_time"]<row["event_time"]]
            day=[r for r in past if r["event_time"]>=row["event_time"]-timedelta(hours=24)]
            hour=[r for r in past if r["event_time"]>=row["event_time"]-timedelta(hours=1)]
            same=[r for r in day if r["payment_currency"]==row["payment_currency"]]
            mean=float(sum(r["payment_amount"] for r in same)/len(same)) if same else 0
            expected=dict(
                log_payment_amount=math.log1p(float(row["payment_amount"])),
                cross_bank=int(row["sender_bank"]!=row["receiver_bank"]),
                send_count_1h=len(hour),send_count_24h=len(day),
                counterparties_24h=len({(r["receiver_bank"],r["receiver_account"]) for r in day}),
                same_currency_count_24h=len(same),log_mean_amount_24h=math.log1p(mean),
                amount_to_history_ratio=float(row["payment_amount"])/mean if mean else 0,
                new_counterparty=int(not any((r["receiver_bank"],r["receiver_account"])==(row["receiver_bank"],row["receiver_account"]) for r in past)),
                cold_start=int(not past),
                history_age_hours=(row["event_time"]-min(r["event_time"] for r in past)).total_seconds()/3600 if past else 0,
                currency_history_missing=int(mean==0),
            )
            for field in NUMERIC:
                with self.subTest(transaction=row["transaction_id"],field=field):
                    self.assertAlmostEqual(actual[row["transaction_id"]][field],expected[field],places=9)
            for field in CATEGORICAL:
                self.assertEqual(actual[row["transaction_id"]][field],row[field])

    def sample(self):
        return [transaction(raw(),1),transaction(raw(time="2022/09/01 00:30", amount="30"),2),transaction(raw(time="2022/09/01 01:00", amount="20",receiver="C"),3),transaction(raw(time="2022/09/01 01:00", amount="40",currency="EUR"),4)]

    def test_reference_counts_and_amounts(self):
        rows=self.sample(); values=calculate(rows)
        for row in rows:
            earlier=[r for r in rows if (r["sender_bank"],r["sender_account"])==(row["sender_bank"],row["sender_account"]) and row["event_time"]-timedelta(hours=24)<=r["event_time"]<row["event_time"]]
            same=[r for r in earlier if r["payment_currency"]==row["payment_currency"]]
            value=values[row["transaction_id"]]
            self.assertEqual(value["send_count_24h"],len(earlier))
            self.assertEqual(value["same_currency_count_24h"],len(same))
            self.assertEqual(value["counterparties_24h"],len({(r["receiver_bank"],r["receiver_account"]) for r in earlier}))
        self.assertEqual(values["sha:3"]["send_count_1h"],2)
        self.assertEqual(values["sha:3"]["amount_to_history_ratio"],1)
        self.assertEqual(values["sha:4"]["currency_history_missing"],1)

    def test_ties_and_future_invariance(self):
        rows=self.sample(); original=calculate(rows)
        shuffled=calculate(list(reversed(rows)))
        self.assertEqual(original,shuffled)
        future=transaction(raw(time="2022/09/03 00:00",amount="99999"),5)
        changed=calculate(rows+[future])
        for key in original: self.assertEqual(original[key],changed[key])

    def test_bank_collision_and_cold_start(self):
        values=calculate([transaction(raw(),1),transaction(raw(time="2022/09/01 01:00",sender_bank="99"),2)])
        self.assertEqual(values["sha:2"]["send_count_24h"],0)
        self.assertEqual(values["sha:2"]["cold_start"],1)

    def test_window_endpoint_and_zero_history(self):
        rows=[transaction(raw(amount="0"),1),transaction(raw(time="2022/09/02 00:00"),2),transaction(raw(time="2022/09/02 00:01"),3)]
        values=calculate(rows)
        self.assertEqual(values["sha:2"]["send_count_24h"],1)
        self.assertEqual(values["sha:2"]["currency_history_missing"],1)
        self.assertEqual(values["sha:3"]["send_count_24h"],1)

    def test_freeze_split_and_no_overwrite(self):
        rows=[transaction(raw(time=f"2022/09/{i:02d} 00:00"),i) for i in range(1,5)]
        config=dict(observation_start="2022-09-01",training_start="2022-09-02",validation_start="2022-09-03",test_start="2022-09-04",observation_end_exclusive="2022-09-05")
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/"f.parquet"; pq.write_table(pa.Table.from_pylist(rows,schema=SCHEMA),path)
            output=Path(root)/"splits.json"
            result=freeze(path,config,output)
            self.assertEqual(sum(v["rows"] for v in result["splits"].values()),4)
            with self.assertRaises(FileExistsError): freeze(path,config,output)
            with self.assertRaises(ValueError): freeze(path,config|{"test_start":"2022-09-01"},Path(root)/"bad.json")


if __name__ == "__main__": unittest.main()

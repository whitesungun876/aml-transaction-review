"""Budget metrics reuse the independently tested ERP rank contract."""
from dataclasses import asdict
from erp_risk.evaluation import ScoredUnit, evaluate_budgets


def evaluate(frame, scores, labels, fractions=(0.01,0.02,0.05)):
    if len(frame)!=len(scores) or len(frame)!=len(labels): raise ValueError("unaligned evaluation")
    units=[ScoredUnit(str(tid),float(score),bool(label)) for tid,score,label in zip(frame.transaction_id,scores,labels,strict=True)]
    overall=[asdict(x) for x in evaluate_budgets(units,fractions)]
    by_day={}
    for unit,time in zip(units,frame.event_time,strict=True):
        by_day.setdefault(str(time.date()),[]).append(unit)
    daily={day:[asdict(x) for x in evaluate_budgets(values,fractions)] for day,values in sorted(by_day.items())}
    aggregate=[]
    for i,fraction in enumerate(fractions):
        hits=sum(values[i]["hits"] for values in daily.values())
        k=sum(values[i]["k"] for values in daily.values())
        positives=sum(values[i]["n_positive"] for values in daily.values())
        aggregate.append({"fraction":fraction,"hits":hits,"reviewed":k,"positives":positives,"precision":hits/k,"recall":hits/positives if positives else None})
    return {"overall":overall,"daily":daily,"daily_micro":aggregate,
            "ap_definition":"ERP deterministic per-positive rank precision mean; not trapezoidal PR-AUC; ID tie order affects AP"}

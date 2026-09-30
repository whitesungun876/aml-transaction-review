"""Review-budget evaluation for a fixed, already scored test batch."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class ScoredUnit:
    unit_id: str
    score: float
    is_positive: bool


@dataclass(frozen=True)
class BudgetResult:
    fraction: float
    n_units: int
    n_positive: int
    k: int
    hits: int
    precision_at_k: float
    recall_at_k: float | None
    random_expected_hits: float
    average_precision: float | None
    cutoff_score: float
    boundary_tie_count: int
    boundary_tie_selected: int
    hits_min_across_ties: int
    hits_max_across_ties: int


def positive_ranks(units: list[ScoredUnit]) -> list[int]:
    """One-based ranks for positive units under the same fixed tie rule."""
    if len({unit.unit_id for unit in units}) != len(units):
        raise ValueError("duplicate review units in scored batch")
    if any(not math.isfinite(unit.score) for unit in units):
        raise ValueError("scores must be finite")
    ranked = sorted(units, key=lambda unit: (-unit.score, unit.unit_id))
    return [rank for rank, unit in enumerate(ranked, start=1) if unit.is_positive]


def evaluate_budgets(
    units: list[ScoredUnit], fractions: tuple[float, ...] = (0.01, 0.02, 0.05)
) -> list[BudgetResult]:
    """Rank high scores first, break ties by unit ID, and report absolute hits.

    Average precision is over the complete scored batch. It is undefined when
    the batch has no positive review units.
    """
    if not units:
        raise ValueError("cannot evaluate an empty batch")
    if len({unit.unit_id for unit in units}) != len(units):
        raise ValueError("duplicate review units in scored batch")
    if any(not math.isfinite(unit.score) for unit in units):
        raise ValueError("scores must be finite")
    if not fractions or any(not 0 < fraction <= 1 for fraction in fractions):
        raise ValueError("review fractions must be within (0, 1]")

    ranked = sorted(units, key=lambda unit: (-unit.score, unit.unit_id))
    n_units = len(ranked)
    n_positive = sum(unit.is_positive for unit in ranked)
    cumulative_hits = 0
    prefix_hits = [0]
    precision_sum = 0.0
    for rank, unit in enumerate(ranked, start=1):
        if unit.is_positive:
            cumulative_hits += 1
            precision_sum += cumulative_hits / rank
        prefix_hits.append(cumulative_hits)
    average_precision = precision_sum / n_positive if n_positive else None

    results = []
    for fraction in fractions:
        k = min(n_units, math.ceil(fraction * n_units))
        hits = prefix_hits[k]
        cutoff_score = ranked[k - 1].score
        tie_count = sum(unit.score == cutoff_score for unit in ranked)
        tie_selected = sum(unit.score == cutoff_score for unit in ranked[:k])
        tie_positives = sum(
            unit.is_positive for unit in ranked if unit.score == cutoff_score
        )
        hits_above_tie = sum(
            unit.is_positive for unit in ranked if unit.score > cutoff_score
        )
        results.append(
            BudgetResult(
                fraction=fraction,
                n_units=n_units,
                n_positive=n_positive,
                k=k,
                hits=hits,
                precision_at_k=hits / k,
                recall_at_k=hits / n_positive if n_positive else None,
                random_expected_hits=k * n_positive / n_units,
                average_precision=average_precision,
                cutoff_score=cutoff_score,
                boundary_tie_count=tie_count,
                boundary_tie_selected=tie_selected,
                hits_min_across_ties=hits_above_tie
                + max(0, tie_selected - (tie_count - tie_positives)),
                hits_max_across_ties=hits_above_tie + min(tie_selected, tie_positives),
            )
        )
    return results

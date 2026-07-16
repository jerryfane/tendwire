"""Translate semantic Claude decisions into private Herdr pane input.

Calibration assumptions are deliberately confined to this backend module:

* Claude Code displays single-choice and plan rows with 1-based decimal
  shortcuts; typing the ordinal and then Enter chooses that row.
* A single-choice write-in row immediately follows the advertised options.
  Its ordinal opens/focuses the text field without an intermediate Enter, so
  Tendwire types ``N + 1`` and then submits the write-in text with Enter.
* Claude Code multi-select digit toggles are not treated as a supported
  contract. Tendwire therefore uses the conservative cursor fallback: the
  cursor starts on row 1, Down moves exactly one row, Enter toggles the current
  option without moving the cursor, and the Submit row immediately follows the
  final option. Selected rows are visited in ascending ordinal order before
  navigating to Submit and pressing Enter.
* Herdr's private ``pane.send_keys`` accepts decimal character keys plus
  ``Down`` and ``Enter``. Write-in prose uses ``pane.send_input`` so Herdr owns
  terminal text encoding and appends the final Enter atomically.

These steps are internal calibration data. They are never accepted from a
connector and there is intentionally no public raw-key command action.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class HerdrDecisionStep:
    """One private, already-calibrated Herdr pane operation."""

    operation: Literal["keys", "input"]
    keys: tuple[str, ...] = ()
    text: str | None = None

    def __post_init__(self) -> None:
        if self.operation == "keys":
            if not self.keys or self.text is not None:
                raise ValueError("key calibration step requires only keys")
        elif self.operation == "input":
            if not isinstance(self.text, str) or not self.text or self.keys != ("Enter",):
                raise ValueError("input calibration step requires text plus Enter")
        else:
            raise ValueError("unsupported decision calibration operation")


def _digit_keys(value: int | str) -> tuple[str, ...]:
    text = str(value)
    if not text.isdigit() or int(text) < 1:
        raise ValueError("decision ordinal must be a positive decimal")
    return tuple(text)


def calibrate_decision_steps(
    *,
    kind: Literal["single", "multi", "plan"],
    option_count: int,
    option_refs: tuple[str, ...] = (),
    text: str | None = None,
) -> tuple[HerdrDecisionStep, ...]:
    """Return private pane operations for one validated semantic selection."""
    if (
        kind not in {"single", "multi", "plan"}
        or not isinstance(option_count, int)
        or isinstance(option_count, bool)
        or option_count < 1
    ):
        raise ValueError("invalid decision calibration context")
    if text is not None:
        if kind != "single" or option_refs or not isinstance(text, str) or not text:
            raise ValueError("invalid decision write-in calibration")
        return (
            HerdrDecisionStep("keys", keys=_digit_keys(option_count + 1)),
            HerdrDecisionStep("input", keys=("Enter",), text=text),
        )
    if not option_refs or len(option_refs) != len(set(option_refs)):
        raise ValueError("decision option refs must be nonempty and unique")
    ordinals: list[int] = []
    for ref in option_refs:
        if not isinstance(ref, str) or not ref.isdigit():
            raise ValueError("decision option ref must be a decimal ordinal")
        ordinal = int(ref)
        if not 1 <= ordinal <= option_count:
            raise ValueError("decision option ref is out of range")
        ordinals.append(ordinal)
    if kind in {"single", "plan"}:
        if len(ordinals) != 1:
            raise ValueError("single and plan decisions require one option")
        return (
            HerdrDecisionStep(
                "keys",
                keys=(*_digit_keys(ordinals[0]), "Enter"),
            ),
        )

    steps: list[HerdrDecisionStep] = []
    current_row = 1
    for ordinal in sorted(ordinals):
        steps.append(
            HerdrDecisionStep(
                "keys",
                keys=(*(("Down",) * (ordinal - current_row)), "Enter"),
            )
        )
        current_row = ordinal
    steps.append(
        HerdrDecisionStep(
            "keys",
            keys=(*(("Down",) * (option_count + 1 - current_row)), "Enter"),
        )
    )
    return tuple(steps)

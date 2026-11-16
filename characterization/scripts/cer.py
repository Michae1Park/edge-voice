"""Character Error Rate (CER) -- no external dependency (no jiwer/etc).

CER = edit_distance(reference_chars, hypothesis_chars) / len(reference_chars),
computed with whitespace stripped from both sides first. Stripping is
deliberate: the hypothesis is a concatenation of separately-decoded VAD
segments, and spacing at those segment boundaries (single space vs none vs
a stray leading/trailing space) is an artifact of how this harness joins
segments, not a transcription error the model actually made -- comparing
whitespace there would penalize segment-boundary formatting, not accuracy.
"""

from __future__ import annotations


def _strip_whitespace(text: str) -> str:
    return "".join(text.split())


def _levenshtein(a: str, b: str) -> int:
    """Classic O(len(a)*len(b)) edit distance, single-row DP."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, start=1):
            cost = 0 if ca == cb else 1
            curr[j] = min(
                prev[j] + 1,  # deletion
                curr[j - 1] + 1,  # insertion
                prev[j - 1] + cost,  # substitution
            )
        prev = curr
    return prev[-1]


def char_cer(reference: str, hypothesis: str) -> float:
    """CER as a fraction (0.0 = perfect, 1.0+ = as many errors as reference chars)."""
    ref = _strip_whitespace(reference)
    hyp = _strip_whitespace(hypothesis)
    if not ref:
        return 0.0 if not hyp else float("inf")
    return _levenshtein(ref, hyp) / len(ref)

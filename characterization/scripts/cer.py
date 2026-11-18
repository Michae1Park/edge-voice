from __future__ import annotations


def _strip_whitespace(text: str) -> str:
    return "".join(text.split())


def _levenshtein(a: str, b: str) -> int:
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
                prev[j] + 1,
                curr[j - 1] + 1,
                prev[j - 1] + cost,
            )
        prev = curr
    return prev[-1]


def char_cer(reference: str, hypothesis: str) -> float:
    ref = _strip_whitespace(reference)
    hyp = _strip_whitespace(hypothesis)
    if not ref:
        return 0.0 if not hyp else float("inf")
    return _levenshtein(ref, hyp) / len(ref)

#!/usr/bin/env python3
"""Enumerate the models moonshine actually publishes, vs. the ones it lists.

There is no browsable index for download.moonshine.ai -- the bare host 404s,
and only exact object keys resolve. The "catalog" is a hardcoded MODEL_INFO
table compiled into the installed moonshine_voice package, so a model can be
published on the CDN and still be unreachable through
get_model_for_language(), which raises ValueError("Model not found...") for
anything missing from that table.

That gap is not hypothetical: it is why stt_worker.py carries
_MODEL_REGISTRY_OVERRIDES for ko/base. When this was last run, 18 models were
published and 13 were listed -- base-ko, tiny-ar, tiny-uk, tiny-vi and tiny-zh
existed but were invisible to the API.

So this script probes the key space directly and diffs it against the
installed registry. Two reasons to re-run it:

  1. After upgrading moonshine-voice, to see whether upstream finally listed
     what it publishes (and whether _MODEL_REGISTRY_OVERRIDES can shrink).
     Note 0.1.0 moved MODEL_INFO into libmoonshine.so, so this script's
     registry side may need adjusting there.
  2. To check whether a *streaming* model exists for a non-English language.
     That is the upstream gate on docs/STREAMING_STT_PLAN.md: streaming stays
     unreachable while the deployment language is Korean, and the STREAMING
     column below is the one-command answer to "has that changed yet?"

Existence only. A 200 here proves the object is served, not that
libmoonshine loads it or that its accuracy is usable -- of the unlisted five,
only base-ko has actually been run.

Usage:
    python scratch/probe_moonshine_models.py           # the 8 known languages
    python scratch/probe_moonshine_models.py --wide    # + plausible new ones
"""

from __future__ import annotations

import argparse
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

CDN = "https://download.moonshine.ai/model"
TIMEOUT_S = 12

# Cloudflare fronts the CDN and 403s urllib's default "Python-urllib/3.12"
# agent -- which would make every probe below look like a missing model.
# Any honest identifier is accepted (moonshine's own downloader gets through
# as python-requests); this is not impersonation, and must not become it.
USER_AGENT = "edge-voice-model-probe/1.0"

# Languages moonshine has published at some point. --wide adds candidates it
# has not, purely to notice a new one appearing before the package does.
KNOWN_LANGUAGES = ["ar", "en", "es", "ja", "ko", "uk", "vi", "zh"]
SPECULATIVE_LANGUAGES = ["de", "fr", "hi", "id", "it", "nl", "pl", "pt", "ru", "th", "tr"]

# Non-streaming archs nest a repeated directory under quantized/; streaming
# ones put the files directly in it. Both layouts are probed per candidate,
# so a new arch that switches layout is still found.
ARCHS = [
    "tiny",
    "base",
    "small",
    "medium",
    "tiny-streaming",
    "base-streaming",
    "small-streaming",
    "medium-streaming",
]


def _exists(url: str) -> bool:
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_S) as response:
            return bool(response.status == 200)
    except urllib.error.HTTPError as exc:
        # 404 is the answer we're probing for; 403 means the request was
        # rejected before the CDN ever looked for the object (see USER_AGENT),
        # so reporting it as "no such model" would be a lie.
        if exc.code not in (403, 429):
            return False
        raise RuntimeError(
            f"CDN rejected the probe with HTTP {exc.code} -- results would be "
            f"wrong, not merely empty. Check USER_AGENT / rate limiting."
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def probe(name: str) -> str | None:
    """The URL prefix serving `name`, or None if the CDN has no such model.

    tokenizer.bin is the probe target because every model ships one, in both
    layouts -- unlike the .ort component files, which differ per arch
    (decoder_with_attention.ort exists only for base-en, for instance).
    """
    for prefix in (f"{CDN}/{name}/quantized/{name}", f"{CDN}/{name}/quantized"):
        if _exists(f"{prefix}/tokenizer.bin"):
            return prefix
    return None


def installed_registry() -> dict[str, str]:
    """{model_name: download_url} as the installed package sees it.

    Returns {} rather than raising when the table can't be read (not
    installed, or moved into the native lib upstream) -- the CDN half of this
    script is still useful on its own.
    """
    try:
        from moonshine_voice.download import MODEL_INFO
    except Exception as exc:  # pragma: no cover - diagnostic script
        print(f"! could not read MODEL_INFO ({exc}) -- reporting CDN contents only\n")
        return {}

    return {
        model["download_url"].rstrip("/").split("/model/")[-1].split("/")[0]: model["download_url"]
        for entry in MODEL_INFO.values()
        for model in entry["models"]
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--wide",
        action="store_true",
        help="also probe languages moonshine does not currently publish",
    )
    args = parser.parse_args()

    languages = KNOWN_LANGUAGES + (SPECULATIVE_LANGUAGES if args.wide else [])
    candidates = [f"{arch}-{lang}" for lang in languages for arch in ARCHS]
    print(f"Probing {len(candidates)} candidate keys against {CDN} ...\n")

    with ThreadPoolExecutor(max_workers=8) as pool:
        found = {
            name: prefix
            for name, prefix in zip(candidates, pool.map(probe, candidates))
            if prefix is not None
        }

    # Registry keys are the CDN directory the URL points at, so a mismatched
    # row (moonshine's ko entry is named "base-ko" but points at tiny-ko)
    # compares by where it actually resolves, not by what it calls itself.
    listed = installed_registry()

    print(f"{'LANGUAGE':<10}{'ARCH':<18}{'STATUS'}")
    print("-" * 46)
    for lang in languages:
        for arch in ARCHS:
            name = f"{arch}-{lang}"
            if name not in found:
                continue
            status = "listed" if name in listed else "PUBLISHED BUT UNLISTED"
            print(f"{lang:<10}{arch:<18}{status}")

    unlisted = sorted(set(found) - set(listed))
    orphaned = sorted(set(listed) - set(found))

    print(f"\n{len(found)} published, {len(listed)} listed by moonshine_voice.")
    if unlisted:
        print(
            f"\nUnlisted ({len(unlisted)}) -- reachable only via "
            f"_MODEL_REGISTRY_OVERRIDES in stt/stt_worker.py:"
        )
        for name in unlisted:
            print(f"  {name:<22}{found[name]}")
    if orphaned:
        # Would mean the installed package points at something withdrawn --
        # a startup failure waiting to happen, so it is worth shouting about.
        print(f"\n! Listed but NOT on the CDN ({len(orphaned)}): {', '.join(orphaned)}")

    streaming_langs = sorted({n.rsplit("-", 1)[1] for n in found if "streaming" in n})
    print(f"\nStreaming archs published for: {', '.join(streaming_langs) or 'nothing'}")
    print("docs/STREAMING_STT_PLAN.md unblocks when the deployment language appears in that list.")


if __name__ == "__main__":
    main()

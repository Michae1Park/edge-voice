#!/usr/bin/env python3
# Enumerate the models moonshine actually publishes, vs. the ones it lists.
# Usage: python scratch/probe_moonshine_models.py

from __future__ import annotations

import argparse
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

CDN = "https://download.moonshine.ai/model"
TIMEOUT_S = 12

USER_AGENT = "edge-voice-model-probe/1.0"

KNOWN_LANGUAGES = ["ar", "en", "es", "ja", "ko", "uk", "vi", "zh"]
SPECULATIVE_LANGUAGES = ["de", "fr", "hi", "id", "it", "nl", "pl", "pt", "ru", "th", "tr"]

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
        if exc.code not in (403, 429):
            return False
        raise RuntimeError(
            f"CDN rejected the probe with HTTP {exc.code} -- results would be "
            f"wrong, not merely empty. Check USER_AGENT / rate limiting."
        ) from exc
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def probe(name: str) -> str | None:
    for prefix in (f"{CDN}/{name}/quantized/{name}", f"{CDN}/{name}/quantized"):
        if _exists(f"{prefix}/tokenizer.bin"):
            return prefix
    return None


def installed_registry() -> dict[str, str]:
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
        print(f"\n! Listed but NOT on the CDN ({len(orphaned)}): {', '.join(orphaned)}")

    streaming_langs = sorted({n.rsplit("-", 1)[1] for n in found if "streaming" in n})
    print(f"\nStreaming archs published for: {', '.join(streaming_langs) or 'nothing'}")
    print(
        "docs/deferred/STREAMING_STT_PLAN.md unblocks when the deployment language appears in that list."
    )


if __name__ == "__main__":
    main()

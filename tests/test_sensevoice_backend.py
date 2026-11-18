import io
import logging
import queue
import subprocess
import tarfile

import numpy as np
import pytest

from edge_voice.config.settings import MQTTChannels, Settings
from edge_voice.pipeline.models import SpeechSegment
from edge_voice.pipeline.orchestrator import PipelineOrchestrator
from edge_voice.stt import stt_worker
from edge_voice.stt.sensevoice import factory, models
from edge_voice.stt.sensevoice import recognizer as sv_recognizer
from edge_voice.stt.sensevoice.config import SenseVoiceConfig
from edge_voice.stt.sensevoice.factory import (
    NoNpuDeviceError,
    SenseVoiceUnavailableError,
    build_sensevoice_transcriber,
)
from edge_voice.stt.stt_worker import STTWorker, STTWorkerConfig
from edge_voice.stt.utterance_transcriber import UtteranceTranscriber


class _FakeRecognizer:
    def __init__(self, model_dir=None, runtime="cpu", config=None, texts=None):
        self.runtime = runtime
        self.calls: list[int] = []
        self._texts = list(texts or [])

    def transcribe(self, samples, sample_rate):
        self.calls.append(len(samples))
        return self._texts.pop(0) if self._texts else "안녕하세요"


class _Collector:
    def __init__(self):
        self.lines = []

    def on_line_completed(self, event):
        self.lines.append(event.line.text)


def _model_dir(root, runtime):
    path = root / models.release_name(runtime, SenseVoiceConfig())
    path.mkdir(parents=True)
    (path / models.MODEL_FILES[runtime]).write_bytes(b"")
    (path / models.TOKENS_FILE).write_text("")
    return path


def _segment(n_samples=16000, partial=False):
    return SpeechSegment(
        channel_id="rx",
        start=0.0,
        end=n_samples / 16000,
        audio=np.full(n_samples, 1000, dtype=np.int16).tobytes(),
        segment_id="rx-1",
        is_partial=partial,
    )


@pytest.fixture
def board(monkeypatch, tmp_path):
    """A board with an NPU, sherpa-onnx installed, and both models cached."""
    _model_dir(tmp_path, "cpu")
    _model_dir(tmp_path, "npu")
    monkeypatch.setattr(factory, "rknpu_present", lambda: True)
    monkeypatch.setattr(factory.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(sv_recognizer, "preload_npu_runtime", lambda lib: None)
    monkeypatch.setattr(sv_recognizer, "SenseVoiceRecognizer", _FakeRecognizer)
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0, "", "")
    )
    return SenseVoiceConfig(cache_dir=str(tmp_path), npu_core=0)


def _probe_exits(monkeypatch, code, stderr="E RKNN: load model failed"):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, code, "", stderr)
    )


class TestUtteranceTranscriber:
    def test_stop_emits_one_completed_line(self):
        transcriber = UtteranceTranscriber(_FakeRecognizer())
        collector = _Collector()
        transcriber.add_listener(collector)
        transcriber.start()
        transcriber.add_audio([0.1] * 16000, 16000)
        transcriber.stop()
        assert collector.lines == ["안녕하세요"]

    def test_audio_longer_than_window_is_chunked_and_joined(self):
        recognizer = _FakeRecognizer(texts=["첫째", "둘째", "셋째"])
        transcriber = UtteranceTranscriber(recognizer, window_s=1.0)
        result = transcriber.transcribe_without_streaming(np.zeros(40000, np.float32), 16000)
        assert recognizer.calls == [16000, 16000, 8000]
        assert [line.text for line in result.lines] == ["첫째 둘째 셋째"]

    def test_no_window_means_one_call(self):
        recognizer = _FakeRecognizer()
        UtteranceTranscriber(recognizer).transcribe_without_streaming(
            np.zeros(40000, np.float32), 16000
        )
        assert recognizer.calls == [40000]

    def test_empty_audio_skips_recognizer_and_emits_nothing(self):
        recognizer = _FakeRecognizer()
        transcriber = UtteranceTranscriber(recognizer)
        collector = _Collector()
        transcriber.add_listener(collector)
        transcriber.start()
        transcriber.stop()
        assert recognizer.calls == []
        assert collector.lines == []

    def test_blank_text_emits_nothing(self):
        transcriber = UtteranceTranscriber(_FakeRecognizer(texts=["  "]))
        collector = _Collector()
        transcriber.add_listener(collector)
        transcriber.start()
        transcriber.add_audio([0.1] * 160, 16000)
        transcriber.stop()
        assert collector.lines == []

    def test_remove_all_listeners_detaches_previous_segment_collector(self):
        transcriber = UtteranceTranscriber(_FakeRecognizer())
        stale = _Collector()
        transcriber.add_listener(stale)
        transcriber.remove_all_listeners()
        transcriber.start()
        transcriber.add_audio([0.1] * 160, 16000)
        transcriber.stop()
        assert stale.lines == []


class TestCoreMask:
    @pytest.mark.parametrize(("core", "mask"), [(None, 1), (0, 0), (1, -1), (2, -2)])
    def test_maps_core_index_to_sherpa_mask(self, core, mask):
        assert sv_recognizer.core_mask(core) == mask

    def test_rejects_core_past_rk3588(self):
        with pytest.raises(ValueError):
            sv_recognizer.core_mask(3)


class TestRuntimeSelection:
    @pytest.mark.parametrize("runtime", ["npu", "auto"])
    def test_npu_used_when_available(self, board, runtime):
        board.runtime = runtime
        built = build_sensevoice_transcriber(board)
        assert built.runtime == "npu"
        assert built.npu_fallback is None
        assert built.transcriber._window_s == 5

    def test_cpu_runtime_never_tries_npu(self, board, monkeypatch):
        board.runtime = "cpu"
        monkeypatch.setattr(factory, "_build_npu", lambda cfg: pytest.fail("NPU tried"))
        built = build_sensevoice_transcriber(board)
        assert built.runtime == "cpu"
        assert built.transcriber._window_s is None

    def test_no_device_falls_back_to_cpu(self, board, monkeypatch):
        monkeypatch.setattr(factory, "rknpu_present", lambda: False)
        built = build_sensevoice_transcriber(board)
        assert built.runtime == "cpu"
        assert isinstance(built.npu_fallback, NoNpuDeviceError)

    def test_probe_crash_falls_back_to_cpu_with_rknn_error(self, board, monkeypatch):
        _probe_exits(monkeypatch, 255)
        built = build_sensevoice_transcriber(board)
        assert built.runtime == "cpu"
        assert "load model failed" in str(built.npu_fallback)

    def test_probe_timeout_falls_back(self, board, monkeypatch):
        def _timeout(*a, **k):
            raise subprocess.TimeoutExpired(a, 1)

        monkeypatch.setattr(subprocess, "run", _timeout)
        assert "timed out" in str(build_sensevoice_transcriber(board).npu_fallback)

    def test_unloadable_runtime_lib_falls_back(self, board, monkeypatch):
        def _fail(lib):
            raise OSError("librknnrt.so: cannot open shared object file")

        monkeypatch.setattr(sv_recognizer, "preload_npu_runtime", _fail)
        assert "librknnrt" in str(build_sensevoice_transcriber(board).npu_fallback)

    def test_missing_runtime_lib_path_falls_back(self, board):
        board.npu_runtime_lib = "/nonexistent/librknnrt.so"
        assert "does not exist" in str(build_sensevoice_transcriber(board).npu_fallback)

    def test_broken_npu_model_override_falls_back(self, board, tmp_path):
        board.npu_model_dir = str(tmp_path / "empty")
        built = build_sensevoice_transcriber(board)
        assert built.runtime == "cpu"
        assert "model.rknn" in str(built.npu_fallback)

    def test_sherpa_missing_refuses_to_start(self, board, monkeypatch):
        monkeypatch.setattr(factory.importlib.util, "find_spec", lambda name: None)
        with pytest.raises(SenseVoiceUnavailableError, match="sherpa-onnx"):
            build_sensevoice_transcriber(board)

    def test_cpu_failure_has_no_further_fallback(self, board, monkeypatch, tmp_path):
        monkeypatch.setattr(factory, "rknpu_present", lambda: False)
        board.cpu_model_dir = str(tmp_path / "empty")
        with pytest.raises(SenseVoiceUnavailableError, match="model.int8.onnx"):
            build_sensevoice_transcriber(board)


def _release_archive(name, runtime):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:bz2") as archive:
        for member in (models.MODEL_FILES[runtime], models.TOKENS_FILE):
            info = tarfile.TarInfo(f"{name}/{member}")
            archive.addfile(info, io.BytesIO(b""))
    buffer.seek(0)
    return buffer


class TestModelDownload:
    def test_downloads_into_cache_on_first_use_only(self, monkeypatch, tmp_path):
        config = SenseVoiceConfig(cache_dir=str(tmp_path))
        name = models.release_name("cpu", config)
        urls = []

        def _urlopen(url, timeout):
            urls.append(url)
            return _release_archive(name, "cpu")

        monkeypatch.setattr(models.urllib.request, "urlopen", _urlopen)
        assert models.ensure_model_dir("cpu", config) == tmp_path / name
        assert models.ensure_model_dir("cpu", config) == tmp_path / name
        assert urls == [
            f"https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/{name}.tar.bz2"
        ]

    def test_npu_release_follows_window(self):
        assert "rk3588-10-seconds" in models.release_name("npu", SenseVoiceConfig(npu_window_s=10))

    def test_failed_download_leaves_no_partial_model(self, monkeypatch, tmp_path):
        def _urlopen(url, timeout):
            raise OSError("network unreachable")

        monkeypatch.setattr(models.urllib.request, "urlopen", _urlopen)
        with pytest.raises(models.ModelUnavailableError, match="network unreachable"):
            models.ensure_model_dir("cpu", SenseVoiceConfig(cache_dir=str(tmp_path)))
        assert [p.name for p in tmp_path.iterdir() if not p.name.endswith(".lock")] == []

    def test_override_dir_is_used_as_is(self, tmp_path):
        path = _model_dir(tmp_path, "npu")
        config = SenseVoiceConfig(npu_model_dir=str(path), cache_dir=str(tmp_path / "unused"))
        assert models.ensure_model_dir("npu", config) == path

    @pytest.mark.parametrize(
        ("npu_present", "runtimes", "expected"),
        [
            (True, ["auto", "auto"], {"npu"}),
            (False, ["auto"], {"cpu"}),
            (True, ["cpu"], {"cpu"}),
            (True, ["auto", "cpu"], {"npu", "cpu"}),
        ],
    )
    def test_prefetch_fetches_what_workers_load_first(
        self, monkeypatch, npu_present, runtimes, expected
    ):
        fetched = set()
        monkeypatch.setattr(factory, "rknpu_present", lambda: npu_present)
        monkeypatch.setattr(factory, "ensure_model_dir", lambda rt, cfg: fetched.add(rt))
        factory.prefetch_models([SenseVoiceConfig(runtime=r) for r in runtimes])
        assert fetched == expected


class TestWorker:
    @staticmethod
    def _worker(config, events=None, model="sensevoice"):
        return STTWorker(
            segment_queue=queue.Queue(),
            on_transcript=(events.append if events is not None else lambda e: None),
            config=STTWorkerConfig(model=model, sensevoice=config),
        )

    def test_sensevoice_is_the_default_model(self):
        assert STTWorkerConfig().model == "sensevoice"
        assert SenseVoiceConfig().runtime == "auto"

    def test_moonshine_never_touches_sensevoice(self, monkeypatch):
        sentinel = object()
        monkeypatch.setattr(STTWorker, "_new_moonshine_transcriber", lambda self: sentinel)
        monkeypatch.setattr(
            "edge_voice.stt.sensevoice.build_sensevoice_transcriber",
            lambda cfg: pytest.fail("SenseVoice built for moonshine"),
        )
        assert self._worker(SenseVoiceConfig(), model="moonshine")._transcriber is sentinel

    @pytest.mark.parametrize(
        ("runtime", "level"), [("npu", logging.WARNING), ("auto", logging.INFO)]
    )
    def test_missing_device_fallback_log_level(self, board, monkeypatch, caplog, runtime, level):
        board.runtime = runtime
        monkeypatch.setattr(factory, "rknpu_present", lambda: False)
        with caplog.at_level(logging.INFO, logger=stt_worker.__name__):
            self._worker(board)
        record = next(r for r in caplog.records if "running SenseVoice on CPU" in r.getMessage())
        assert record.levelno == level
        assert "no RKNPU device" in record.getMessage()

    @pytest.mark.parametrize("runtime", ["npu", "auto"])
    def test_npu_load_failure_warns_even_on_auto(self, board, monkeypatch, caplog, runtime):
        board.runtime = runtime
        _probe_exits(monkeypatch, 1)
        with caplog.at_level(logging.INFO, logger=stt_worker.__name__):
            self._worker(board)
        record = next(r for r in caplog.records if "running SenseVoice on CPU" in r.getMessage())
        assert record.levelno == logging.WARNING

    def test_runtime_is_logged(self, board, caplog):
        with caplog.at_level(logging.INFO, logger=stt_worker.__name__):
            self._worker(board)
        assert any(
            "model=sensevoice runtime=npu npu_core=0" in r.getMessage() for r in caplog.records
        )

    def test_segment_flows_through_worker_to_transcript(self, board):
        events = []
        worker = self._worker(board, events)
        worker._handle_segment(_segment())
        worker._handle_segment(_segment(partial=True))
        assert [(e.text, e.is_final) for e in events] == [
            ("안녕하세요", True),
            ("안녕하세요", False),
        ]
        assert worker.last_latency_s is not None


def _channels(n):
    return {"channels": [MQTTChannels(topic=f"t{i}", channel_id=f"c{i}") for i in range(n)]}


def _stt_configs(settings):
    orchestrator = PipelineOrchestrator.__new__(PipelineOrchestrator)
    orchestrator._settings = settings
    return [orchestrator._stt_config(c.channel_id) for c in settings.mqtt.channels]


class TestSettings:
    def test_defaults(self):
        stt = Settings().stt
        assert (stt.model, stt.runtime, stt.sensevoice.npu_max_channels) == (
            "sensevoice",
            "auto",
            3,
        )

    def test_moonshine_on_npu_is_rejected(self):
        with pytest.raises(ValueError, match="CPU only"):
            Settings(stt={"model": "moonshine", "runtime": "npu"})

    def test_moonshine_on_auto_is_allowed(self):
        assert Settings(stt={"model": "moonshine", "runtime": "auto"}).stt.model == "moonshine"

    def test_sensevoice_rejects_unsupported_language(self):
        with pytest.raises(ValueError, match="not supported by SenseVoice"):
            Settings(stt={"language": "es"})

    def test_moonshine_keeps_its_own_languages(self):
        assert Settings(stt={"model": "moonshine", "language": "es"}).stt.language == "es"

    def test_stale_keys_fail_loudly(self):
        with pytest.raises(ValueError, match="backend"):
            Settings(stt={"backend": "npu"})
        with pytest.raises(ValueError, match="model_arch"):
            Settings(stt={"model_arch": "tiny"})

    def test_npu_runtime_rejects_more_channels_than_cores(self):
        with pytest.raises(ValueError, match="npu_max_channels=3"):
            Settings(mqtt=_channels(4), stt={"runtime": "npu"})

    def test_cap_is_configurable_downward_only(self):
        with pytest.raises(ValueError, match="npu_max_channels=2"):
            Settings(
                mqtt=_channels(3), stt={"runtime": "npu", "sensevoice": {"npu_max_channels": 2}}
            )
        with pytest.raises(ValueError):
            Settings(stt={"sensevoice": {"npu_max_channels": 4}})

    def test_cap_ignores_cpu_and_moonshine(self):
        Settings(mqtt=_channels(5), stt={"runtime": "cpu"})
        Settings(mqtt=_channels(5), stt={"model": "moonshine"})

    def test_each_npu_channel_gets_its_own_core(self):
        configs = _stt_configs(Settings(mqtt=_channels(3), stt={"runtime": "npu"}))
        assert [c.sensevoice.npu_core for c in configs] == [0, 1, 2]
        assert {c.sensevoice.runtime for c in configs} == {"npu"}

    def test_auto_puts_channels_past_the_cap_on_cpu(self):
        configs = _stt_configs(Settings(mqtt=_channels(5)))
        assert [(c.sensevoice.runtime, c.sensevoice.npu_core) for c in configs] == [
            ("auto", 0),
            ("auto", 1),
            ("auto", 2),
            ("cpu", None),
            ("cpu", None),
        ]

    def test_moonshine_options_come_from_their_own_block(self):
        configs = _stt_configs(
            Settings(stt={"model": "moonshine", "moonshine": {"max_tokens_per_second": "30.0"}})
        )
        assert configs[0].model == "moonshine"
        assert configs[0].options["max_tokens_per_second"] == "30.0"


class TestCpuOnRknnBuild:
    def test_cpu_preloads_configured_npu_runtime(self, monkeypatch, tmp_path):
        loaded = []
        monkeypatch.setattr(sv_recognizer, "preload_npu_runtime", loaded.append)
        monkeypatch.setitem(__import__("sys").modules, "sherpa_onnx", _FakeSherpa)
        config = SenseVoiceConfig(runtime="cpu", npu_runtime_lib="/opt/librknnrt.so")
        sv_recognizer.SenseVoiceRecognizer(tmp_path, "cpu", config)
        assert loaded == ["/opt/librknnrt.so"]

    def test_cpu_skips_preload_without_configured_runtime(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            sv_recognizer, "preload_npu_runtime", lambda lib: pytest.fail("preloaded")
        )
        monkeypatch.setitem(__import__("sys").modules, "sherpa_onnx", _FakeSherpa)
        sv_recognizer.SenseVoiceRecognizer(tmp_path, "cpu", SenseVoiceConfig(runtime="cpu"))

    def test_import_failure_becomes_clear_startup_error(self, board, monkeypatch):
        def _fail(*a, **k):
            raise ImportError("librknnrt.so: cannot open shared object file")

        monkeypatch.setattr(sv_recognizer, "SenseVoiceRecognizer", _fail)
        board.runtime = "cpu"
        with pytest.raises(SenseVoiceUnavailableError, match="npu_runtime_lib"):
            build_sensevoice_transcriber(board)


class _FakeSherpa:
    class OfflineRecognizer:
        @staticmethod
        def from_sense_voice(**kwargs):
            return object()

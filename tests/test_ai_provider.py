from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from policydb.ai import SiliconFlowProvider
from policydb.config.preferences import PreferencesStore
from policydb.settings import Settings


class Result(BaseModel):
    label: str


class FakeClient:
    def __init__(self):
        self.models = SimpleNamespace(
            list=lambda: SimpleNamespace(
                data=[
                    SimpleNamespace(id="chat-model"),
                    SimpleNamespace(id="BAAI/bge-m3"),
                    SimpleNamespace(id="BAAI/bge-reranker-v2-m3"),
                ]
            )
        )
        completion = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content=json.dumps({"label": "D06"}))
                )
            ],
            usage=SimpleNamespace(prompt_tokens=10, completion_tokens=3),
            _request_id="trace-1",
        )
        self.chat = SimpleNamespace(
            completions=SimpleNamespace(create=lambda **_: completion)
        )


def test_siliconflow_models_and_structured_output(tmp_path):
    settings = Settings(root=tmp_path)
    provider = SiliconFlowProvider(settings, client=FakeClient())
    assert provider.models()[0] == "BAAI/bge-m3"
    result, trace = provider.structured(
        model="chat-model", system="system", user="text", schema=Result
    )
    assert result.label == "D06"
    assert trace.trace_id == "trace-1"


def test_siliconflow_model_test_reports_unavailable(tmp_path, monkeypatch):
    monkeypatch.setenv("SILICONFLOW_CHAT_MODEL", "missing-model")
    provider = SiliconFlowProvider(Settings(root=tmp_path), client=FakeClient())
    assert provider.test()["unavailable_models"] == ["missing-model"]


def test_preferences_cannot_store_siliconflow_key(tmp_path):
    store = PreferencesStore(tmp_path / "preferences.json")
    with pytest.raises(ValueError):
        store.save({"siliconflow_api_key": "secret"})
    assert not store.path.exists()

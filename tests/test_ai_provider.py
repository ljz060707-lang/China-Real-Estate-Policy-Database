from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from pydantic import BaseModel

from policydb.ai import SiliconFlowProvider, validate_structured_payload
from policydb.config.preferences import PreferencesStore
from policydb.episode_930 import ActionClassificationPayload
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
    assert trace.configured_read_timeout == 30.0
    assert trace.configured_connect_timeout == 10.0
    assert trace.max_retries == 3


def test_siliconflow_timeout_override_is_auditable_without_network(tmp_path):
    provider = SiliconFlowProvider(
        Settings(root=tmp_path),
        client=FakeClient(),
        request_timeout_override=300,
        connect_timeout_override=10,
        max_retries_override=0,
    )

    assert provider.configured_read_timeout == 300.0
    assert provider.configured_connect_timeout == 10.0
    assert provider.configured_max_retries == 0


def test_siliconflow_model_test_reports_unavailable(tmp_path, monkeypatch):
    monkeypatch.setenv("SILICONFLOW_CHAT_MODEL", "missing-model")
    provider = SiliconFlowProvider(Settings(root=tmp_path), client=FakeClient())
    assert provider.test()["unavailable_models"] == ["missing-model"]


def test_preferences_cannot_store_siliconflow_key(tmp_path):
    store = PreferencesStore(tmp_path / "preferences.json")
    with pytest.raises(ValueError):
        store.save({"siliconflow_api_key": "secret"})
    assert not store.path.exists()


def test_episode_schema_normalizes_saved_structural_wrapper() -> None:
    payload = {
        "classification_output": {
            "actions": [
                {
                    "action_id": "A1",
                    "policy_type": "LIMIT_PURCHASE",
                    "direction": "TIGHTENING",
                }
            ]
        }
    }

    parsed = validate_structured_payload(payload, ActionClassificationPayload)

    assert parsed.actions[0].action_id == "A1"
    assert parsed.actions[0].policy_type == "LIMIT_PURCHASE"


def test_episode_schema_normalizes_saved_classified_actions_wrapper() -> None:
    payload = {
        "classified_actions": [
            {
                "action_id": "A2",
                "policy_type": "CREDIT_TIGHTENING",
                "direction": "TIGHTENING",
                "confidence": 0.9,
            }
        ]
    }

    parsed = validate_structured_payload(payload, ActionClassificationPayload)

    assert parsed.actions[0].action_id == "A2"
    assert parsed.actions[0].confidence == 0.9

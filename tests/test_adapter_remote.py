"""本機 broker agent（WS 通道）例外分類測試。

`AgentUnavailableError`（指令送出前 agent 即不在線，保證未送達券商）
→ `_classify_place_failure` 判 `"failed"`（可安全退配額）。
`AgentCommandTimeoutError`（指令可能已送達但未收到 ack）
→ `_classify_place_failure` 判 `"unknown"`（保守保留配額）。
既有 `code: 4xx` 券商拒單規則不變。
"""
from quanquant.broker.base import AgentCommandTimeoutError, AgentUnavailableError
from quanquant.broker.shioaji_adapter import _classify_place_failure


def test_agent_unavailable_classified_failed():
    assert _classify_place_failure(AgentUnavailableError("agent 未連線")) == "failed"


def test_agent_timeout_classified_unknown():
    assert _classify_place_failure(AgentCommandTimeoutError("ack 逾時")) == "unknown"


def test_broker_reject_code_still_failed():
    assert _classify_place_failure(Exception("code: 406 not signed")) == "failed"

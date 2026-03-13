from __future__ import annotations

from api.models.knowledge_models import GateDecision, RawDocument


class KnowledgeGate:
    def evaluate(self, doc: RawDocument) -> GateDecision:
        text = (doc.raw_content or "").strip()
        title = (doc.title or "").strip()
        source_level = (doc.source_level or "L4").upper()

        if not title or not text:
            return GateDecision(False, "NoiseCache", "missing_title_or_content")

        if source_level == "L4":
            return GateDecision(False, "NoiseCache", "low_trust_source", tags=["unverified"])

        if self._looks_like_forwarding_only(text):
            return GateDecision(False, "NoiseCache", "forwarding_without_new_information")

        if self._is_rule_like(doc):
            return GateDecision(True, "CoreKnowledge", "rule_or_policy_document", tags=["rule"])
        if self._is_strategy_or_case_like(doc):
            return GateDecision(True, "ReferenceKnowledge", "strategy_or_case_document", tags=["strategy_or_case"])

        return GateDecision(True, "ReferenceKnowledge", "generic_reference_document", tags=["reference"])

    @staticmethod
    def _looks_like_forwarding_only(text: str) -> bool:
        low = text.lower()
        bad_patterns = ["转载", "来源：网络", "仅供参考", "不构成投资建议"]
        return len(text) < 80 and any(p in low for p in bad_patterns)

    @staticmethod
    def _is_rule_like(doc: RawDocument) -> bool:
        title = (doc.title or "").lower()
        text = (doc.raw_content or "").lower()
        keywords = ["规则", "办法", "通知", "细则", "问答", "披露", "管理", "指引", "答记者问"]
        return any(k in title or k in text for k in keywords)

    @staticmethod
    def _is_strategy_or_case_like(doc: RawDocument) -> bool:
        title = (doc.title or "").lower()
        text = (doc.raw_content or "").lower()
        keywords = ["复盘", "案例", "策略", "模板", "框架", "机制", "传导"]
        return any(k in title or k in text for k in keywords)

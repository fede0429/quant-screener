from __future__ import annotations

from uuid import uuid4
from api.models.knowledge_models import CleanDocument, KnowledgeUnit


class KnowledgeUnitExtractor:
    def extract(self, doc: CleanDocument) -> list[KnowledgeUnit]:
        if not doc.fragments:
            return []

        units = []
        if doc.doc_type in {"rule", "policy"}:
            units.append(self._build_rule_like_unit(doc))
        elif doc.doc_type in {"strategy", "case"}:
            units.append(self._build_strategy_like_unit(doc))
        else:
            units.append(self._build_reference_unit(doc))
        return units

    def _build_rule_like_unit(self, doc: CleanDocument) -> KnowledgeUnit:
        return KnowledgeUnit(
            knowledge_id=str(uuid4()),
            doc_id=doc.doc_id,
            knowledge_type="RuleKnowledge",
            title=doc.title,
            summary=doc.fragments[0][:120],
            source_name=doc.source_name,
            source_level=doc.source_level,
            confidence_score=0.9 if doc.source_level in {"L1", "L2"} else 0.6,
            structure={
                "doc_type": doc.doc_type,
                "constraints": doc.fragments[:3],
                "decision_impact_modules": ["risk", "execution", "policy_agent"],
            },
        )

    def _build_strategy_like_unit(self, doc: CleanDocument) -> KnowledgeUnit:
        return KnowledgeUnit(
            knowledge_id=str(uuid4()),
            doc_id=doc.doc_id,
            knowledge_type="StrategyTemplateKnowledge" if doc.doc_type == "strategy" else "CaseKnowledge",
            title=doc.title,
            summary=doc.fragments[0][:120],
            source_name=doc.source_name,
            source_level=doc.source_level,
            confidence_score=0.75 if doc.source_level in {"L1", "L2", "L3"} else 0.4,
            structure={
                "doc_type": doc.doc_type,
                "key_fragments": doc.fragments[:5],
                "decision_impact_modules": ["meta_agent", "shadow_learning", "strategy_kb"],
            },
        )

    def _build_reference_unit(self, doc: CleanDocument) -> KnowledgeUnit:
        return KnowledgeUnit(
            knowledge_id=str(uuid4()),
            doc_id=doc.doc_id,
            knowledge_type="ReferenceKnowledge",
            title=doc.title,
            summary=doc.fragments[0][:120],
            source_name=doc.source_name,
            source_level=doc.source_level,
            confidence_score=0.5,
            structure={
                "doc_type": doc.doc_type,
                "key_fragments": doc.fragments[:3],
            },
        )

from __future__ import annotations

from api.document_cleaner import DocumentCleaner
from api.knowledge_gate import KnowledgeGate
from api.knowledge_unit_extractor import KnowledgeUnitExtractor
from api.models.knowledge_models import RawDocument


class KnowledgePipeline:
    def __init__(self) -> None:
        self.gate = KnowledgeGate()
        self.cleaner = DocumentCleaner()
        self.extractor = KnowledgeUnitExtractor()

    def run(self, raw_doc: RawDocument) -> dict:
        decision = self.gate.evaluate(raw_doc)
        if not decision.accepted:
            return {
                "accepted": False,
                "gate_decision": {
                    "level": decision.level,
                    "reason": decision.reason,
                    "tags": decision.tags,
                },
                "clean_doc": None,
                "knowledge_units": [],
            }

        clean_doc = self.cleaner.clean(raw_doc)
        units = self.extractor.extract(clean_doc)
        return {
            "accepted": True,
            "gate_decision": {
                "level": decision.level,
                "reason": decision.reason,
                "tags": decision.tags,
            },
            "clean_doc": {
                "doc_id": clean_doc.doc_id,
                "doc_type": clean_doc.doc_type,
                "fragment_count": len(clean_doc.fragments),
            },
            "knowledge_units": [
                {
                    "knowledge_id": u.knowledge_id,
                    "knowledge_type": u.knowledge_type,
                    "title": u.title,
                    "summary": u.summary,
                    "confidence_score": u.confidence_score,
                    "structure": u.structure,
                }
                for u in units
            ],
        }

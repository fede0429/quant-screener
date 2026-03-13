from __future__ import annotations

from typing import Dict, List

from api.dedup_engine import DedupEngine
from api.knowledge_store import KnowledgeStore
from api.version_resolver import VersionResolver


class KnowledgeWriteService:
    def __init__(
        self,
        store: KnowledgeStore | None = None,
        version_resolver: VersionResolver | None = None,
        dedup_engine: DedupEngine | None = None,
    ) -> None:
        self.store = store or KnowledgeStore()
        self.version_resolver = version_resolver or VersionResolver()
        self.dedup_engine = dedup_engine or DedupEngine()

    def write(self, clean_doc: Dict, knowledge_units: List[Dict]) -> Dict:
        existing_docs = self.store.list_documents()

        dedup = self.dedup_engine.is_duplicate(
            title=clean_doc.get("title", ""),
            clean_content=clean_doc.get("clean_content", ""),
            existing_docs=existing_docs,
        )
        if dedup["duplicate"]:
            return {
                "written": False,
                "reason": "duplicate_document",
                "matched_doc_id": dedup["matched_doc_id"],
                "document": None,
                "units": [],
            }

        version = self.version_resolver.resolve(
            title=clean_doc.get("title", ""),
            source_name=clean_doc.get("source_name", ""),
            existing_docs=existing_docs,
        )

        doc_record = {
            **clean_doc,
            "canonical_doc_id": version["canonical_doc_id"] or clean_doc.get("doc_id"),
            "version_no": version["version_no"],
            "supersedes_doc_id": version["supersedes_doc_id"],
        }
        self.store.add_document(doc_record)

        unit_records = []
        for unit in knowledge_units:
            unit_records.append({
                **unit,
                "doc_id": clean_doc.get("doc_id"),
            })
        self.store.add_units(unit_records)

        return {
            "written": True,
            "reason": "ok",
            "document": doc_record,
            "units": unit_records,
        }

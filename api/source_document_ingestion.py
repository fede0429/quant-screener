from __future__ import annotations

from typing import Any, Dict, List
from uuid import uuid4

from api.models.knowledge_models import RawDocument


class SourceDocumentIngestion:
    def ingest_rows(self, rows: List[Dict[str, Any]]) -> List[RawDocument]:
        docs: List[RawDocument] = []
        for row in rows:
            docs.append(
                RawDocument(
                    doc_id=row.get("doc_id") or str(uuid4()),
                    source_name=row.get("source_name", ""),
                    source_level=row.get("source_level", "L4"),
                    title=row.get("title", ""),
                    source_url=row.get("source_url", ""),
                    publish_time=row.get("publish_time", ""),
                    raw_content=row.get("raw_content", ""),
                    metadata=dict(row.get("metadata", {})),
                )
            )
        return docs

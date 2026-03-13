from __future__ import annotations

from typing import Any, Dict, List

from api.knowledge_pipeline import KnowledgePipeline
from api.knowledge_write_service import KnowledgeWriteService
from api.kb_review_queue import KBReviewQueue
from api.source_document_ingestion import SourceDocumentIngestion


class KnowledgeTaskRunner:
    def __init__(
        self,
        ingestion: SourceDocumentIngestion | None = None,
        pipeline: KnowledgePipeline | None = None,
        write_service: KnowledgeWriteService | None = None,
        review_queue: KBReviewQueue | None = None,
    ) -> None:
        self.ingestion = ingestion or SourceDocumentIngestion()
        self.pipeline = pipeline or KnowledgePipeline()
        self.write_service = write_service or KnowledgeWriteService()
        self.review_queue = review_queue or KBReviewQueue()

    def run_rows(self, rows: List[Dict[str, Any]]) -> Dict:
        docs = self.ingestion.ingest_rows(rows)
        results = []

        for doc in docs:
            pipe_result = self.pipeline.run(doc)
            if not pipe_result["accepted"]:
                self.review_queue.enqueue(
                    "gate_rejected",
                    {
                        "doc_id": doc.doc_id,
                        "title": doc.title,
                        "reason": pipe_result["gate_decision"]["reason"],
                        "source_name": doc.source_name,
                    },
                )
                results.append({
                    "doc_id": doc.doc_id,
                    "accepted": False,
                    "reason": pipe_result["gate_decision"]["reason"],
                })
                continue

            clean_doc = {
                "doc_id": doc.doc_id,
                "source_name": doc.source_name,
                "source_level": doc.source_level,
                "title": doc.title,
                "clean_content": pipe_result["clean_doc"].get("fragment_count", 0) and "",  # placeholder
            }

            # cleaner 的完整 clean_content 当前不在 pipeline 输出里，先用原始文本占位接通写入链
            clean_doc["clean_content"] = doc.raw_content

            write_result = self.write_service.write(
                clean_doc=clean_doc,
                knowledge_units=pipe_result["knowledge_units"],
            )

            if write_result["written"]:
                self.review_queue.enqueue(
                    "knowledge_written_review",
                    {
                        "doc_id": doc.doc_id,
                        "title": doc.title,
                        "unit_count": len(write_result["units"]),
                    },
                )
            else:
                self.review_queue.enqueue(
                    "knowledge_write_rejected",
                    {
                        "doc_id": doc.doc_id,
                        "title": doc.title,
                        "reason": write_result["reason"],
                    },
                )

            results.append({
                "doc_id": doc.doc_id,
                "accepted": True,
                "written": write_result["written"],
                "unit_count": len(write_result.get("units", [])),
            })

        return {
            "doc_count": len(docs),
            "results": results,
            "review_queue_count": self.review_queue.snapshot()["count"],
            "store_snapshot": self.write_service.store.snapshot(),
        }

from __future__ import annotations

from api.kb_review_queue import KBReviewQueue
from api.knowledge_pipeline import KnowledgePipeline
from api.knowledge_task_runner import KnowledgeTaskRunner
from api.knowledge_write_service import KnowledgeWriteService
from api.source_document_ingestion import SourceDocumentIngestion


def build_knowledge_runtime():
    review_queue = KBReviewQueue()
    write_service = KnowledgeWriteService()
    pipeline = KnowledgePipeline()
    ingestion = SourceDocumentIngestion()
    runner = KnowledgeTaskRunner(
        ingestion=ingestion,
        pipeline=pipeline,
        write_service=write_service,
        review_queue=review_queue,
    )
    return {
        "review_queue": review_queue,
        "write_service": write_service,
        "pipeline": pipeline,
        "ingestion": ingestion,
        "runner": runner,
    }

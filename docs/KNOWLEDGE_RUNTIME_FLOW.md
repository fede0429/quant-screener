# 知识系统第一阶段流程

Raw Rows
-> SourceDocumentIngestion
-> RawDocument

RawDocument
-> KnowledgeGate
-> DocumentCleaner
-> KnowledgeUnitExtractor
-> Knowledge Units

Clean Document / Units
-> VersionResolver
-> DedupEngine
-> KnowledgeStore
-> KnowledgeWriteService

Task Flow
-> KnowledgeTaskRunner
-> KBReviewQueue
-> RuntimeService

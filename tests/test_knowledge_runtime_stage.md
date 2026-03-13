# 知识系统第一阶段收口包联调清单

## 1. 新增文件
- `api/models/knowledge_models.py`
- `api/knowledge_gate.py`
- `api/document_cleaner.py`
- `api/knowledge_unit_extractor.py`
- `api/knowledge_pipeline.py`
- `api/version_resolver.py`
- `api/dedup_engine.py`
- `api/knowledge_store.py`
- `api/knowledge_write_service.py`
- `api/source_document_ingestion.py`
- `api/kb_review_queue.py`
- `api/knowledge_task_runner.py`
- `api/knowledge_runtime_service.py`

## 2. 核心检查
### A. Gate / Cleaner / Extractor
- 文档能被正确判断
- 清洗后能分类
- 能输出 KnowledgeUnit

### B. Version / Dedup / Store
- 重复文档能识别
- 同来源同标题能递增版本
- 文档和单元能写入 store

### C. Task Runner / Review Queue
- 批量 rows 可处理
- gate reject 会入 review queue
- write result 会入 review queue

## 3. 下一步建议
这包并入后，下一步最合适的是：
- 做知识系统总整合包
- 或开始接真实 source clients / db backend

# 知识系统第一阶段并包指南

## 推荐顺序
直接以本收口包为准，不再回头逐个拼 3 个知识系统散包。

## 建议步骤
1. 新建分支：
   `feature/knowledge-runtime-round1`
2. 解压本包
3. 合并：
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
4. 做一次本地联调
5. 检查输出：
   - gate_decision
   - clean_doc
   - knowledge_units
   - store_snapshot
   - review_queue snapshot
6. 再合回主线

## 联调重点
### 文档准入
- L4 噪音被拦截
- 规则型文档进入 Core / Reference

### 清洗与抽取
- 噪音段落被清理
- 文档能分类成 rule / policy / strategy / case / reference
- 能输出对应 KnowledgeUnit

### 存储
- 去重和版本识别可工作
- 文档和知识单元可写入 store

### 任务流
- 批量 rows 可跑通
- rejected / written 会进入 review queue

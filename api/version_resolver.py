from __future__ import annotations

from typing import Dict, Optional


class VersionResolver:
    def resolve(self, title: str, source_name: str, existing_docs: list[dict]) -> Dict:
        """基于标题和来源做简化版版本识别。

        返回：
        - is_new_document
        - canonical_doc_id
        - version_no
        - supersedes_doc_id
        """
        normalized_title = self._normalize(title)
        same_group = [
            d for d in existing_docs
            if self._normalize(d.get("title", "")) == normalized_title
            and (d.get("source_name") == source_name)
        ]
        if not same_group:
            return {
                "is_new_document": True,
                "canonical_doc_id": None,
                "version_no": 1,
                "supersedes_doc_id": None,
            }

        latest = sorted(same_group, key=lambda x: int(x.get("version_no", 1)), reverse=True)[0]
        return {
            "is_new_document": True,
            "canonical_doc_id": latest.get("canonical_doc_id") or latest.get("doc_id"),
            "version_no": int(latest.get("version_no", 1)) + 1,
            "supersedes_doc_id": latest.get("doc_id"),
        }

    @staticmethod
    def _normalize(text: str) -> str:
        return "".join((text or "").strip().lower().split())

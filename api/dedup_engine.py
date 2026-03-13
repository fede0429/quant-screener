from __future__ import annotations

from typing import Dict, Optional


class DedupEngine:
    def is_duplicate(self, title: str, clean_content: str, existing_docs: list[dict]) -> Dict:
        normalized_title = self._normalize(title)
        normalized_content = self._normalize(clean_content)[:300]

        for doc in existing_docs:
            if self._normalize(doc.get("title", "")) == normalized_title:
                if self._normalize(doc.get("clean_content", ""))[:300] == normalized_content:
                    return {
                        "duplicate": True,
                        "matched_doc_id": doc.get("doc_id"),
                        "reason": "same_title_same_prefix_content",
                    }

        return {
            "duplicate": False,
            "matched_doc_id": None,
            "reason": "not_duplicate",
        }

    @staticmethod
    def _normalize(text: str) -> str:
        return "".join((text or "").strip().lower().split())

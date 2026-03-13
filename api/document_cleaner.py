from __future__ import annotations

import re
from api.models.knowledge_models import CleanDocument, RawDocument


class DocumentCleaner:
    def clean(self, doc: RawDocument) -> CleanDocument:
        text = doc.raw_content or ""
        text = self._remove_noise_lines(text)
        text = self._normalize_whitespace(text)
        doc_type = self._classify(text, doc.title)
        fragments = self._extract_fragments(text)

        return CleanDocument(
            doc_id=doc.doc_id,
            source_name=doc.source_name,
            source_level=doc.source_level,
            title=doc.title,
            clean_content=text,
            doc_type=doc_type,
            fragments=fragments,
            metadata=dict(doc.metadata),
        )

    @staticmethod
    def _remove_noise_lines(text: str) -> str:
        lines = []
        banned = ["免责声明", "推荐阅读", "上一篇", "下一篇", "扫码", "联系我们", "版权所有"]
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if any(b in line for b in banned):
                continue
            lines.append(line)
        return "\n".join(lines)

    @staticmethod
    def _normalize_whitespace(text: str) -> str:
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    @staticmethod
    def _classify(text: str, title: str) -> str:
        combined = f"{title}\n{text}".lower()
        if any(k in combined for k in ["规则", "细则", "通知", "办法", "问答"]):
            return "rule"
        if any(k in combined for k in ["政策", "指导意见", "支持", "推动"]):
            return "policy"
        if any(k in combined for k in ["复盘", "案例"]):
            return "case"
        if any(k in combined for k in ["策略", "模板", "框架"]):
            return "strategy"
        return "reference"

    @staticmethod
    def _extract_fragments(text: str) -> list[str]:
        result = []
        for part in text.split("\n"):
            part = part.strip()
            if len(part) >= 20:
                result.append(part)
        return result[:20]

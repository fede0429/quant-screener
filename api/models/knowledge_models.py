from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class RawDocument:
    doc_id: str
    source_name: str
    source_level: str
    title: str
    source_url: str = ""
    publish_time: str = ""
    raw_content: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GateDecision:
    accepted: bool
    level: str
    reason: str
    tags: List[str] = field(default_factory=list)


@dataclass
class CleanDocument:
    doc_id: str
    source_name: str
    source_level: str
    title: str
    clean_content: str
    doc_type: str
    fragments: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KnowledgeUnit:
    knowledge_id: str
    doc_id: str
    knowledge_type: str
    title: str
    summary: str
    source_name: str
    source_level: str
    confidence_score: float
    structure: Dict[str, Any] = field(default_factory=dict)

from __future__ import annotations

from typing import Dict, List


class KnowledgeStore:
    def __init__(self) -> None:
        self.documents: List[Dict] = []
        self.units: List[Dict] = []

    def list_documents(self) -> List[Dict]:
        return list(self.documents)

    def list_units(self) -> List[Dict]:
        return list(self.units)

    def add_document(self, doc: Dict) -> Dict:
        self.documents.append(doc)
        return doc

    def add_units(self, units: List[Dict]) -> List[Dict]:
        self.units.extend(units)
        return units

    def snapshot(self) -> Dict:
        return {
            "document_count": len(self.documents),
            "unit_count": len(self.units),
            "documents": list(self.documents),
            "units": list(self.units),
        }

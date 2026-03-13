from __future__ import annotations

from typing import Dict, List
from uuid import uuid4


class KBReviewQueue:
    def __init__(self) -> None:
        self.items: List[Dict] = []

    def enqueue(self, review_type: str, payload: Dict) -> Dict:
        item = {
            "review_id": str(uuid4()),
            "review_type": review_type,
            "status": "pending",
            "payload": payload,
        }
        self.items.append(item)
        return item

    def list_items(self) -> List[Dict]:
        return list(self.items)

    def snapshot(self) -> Dict:
        return {
            "count": len(self.items),
            "items": list(self.items),
        }

from __future__ import annotations

from typing import Callable, Iterable, List

from src.checkpoint import CheckpointStore
from src.ingestion.hubspot import HubSpotIngest


class Customer360PipelineRunner:
    def __init__(self, checkpoint_store: CheckpointStore | None = None):
        self.checkpoint_store = checkpoint_store or CheckpointStore()

    def _tasks(self) -> List[tuple[str, Callable[[], None]]]:
        # HubSpot is CONTACTS ONLY. Orders are owned by OMS; Products are owned by PIM.
        return [
            ("hubspot_contacts", lambda: HubSpotIngest().run()),
        ]

    def run_hourly(self) -> None:
        for _, task in self._tasks():
            task()

    def run_full_load(self) -> None:
        for _, task in self._tasks():
            task()

from dataclasses import dataclass

@dataclass
class QueueStats:
    total: int
    created: int
    running: int
    failed: int
    finished: int

@dataclass
class QueueData:
    url: str
    depth: int
    priority: int

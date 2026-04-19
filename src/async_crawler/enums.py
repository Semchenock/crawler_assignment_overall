from enum import Enum

class FetchResultStatus(Enum):
    FINISHED = 'FINISHED'
    FAILED = 'FAILED'

class ErrorTypes(Enum):
    BLOCKED_BY_ROBOTS = 'BLOCKED_BY_ROBOTS'
    HTTP = 'HTTP'
    TIMEOUT = 'TIMEOUT'
    NETWORK = 'NETWORK'

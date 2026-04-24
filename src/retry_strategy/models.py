from dataclasses import dataclass
from typing import Optional, Union


class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


class NetworkError(Exception):
    pass


class ParseError(Exception):
    pass


ErrorType = Union[TransientError, PermanentError, NetworkError, ParseError]


@dataclass
class RetryRule:
    max_retries: Optional[int]
    base_delay: Optional[float]
    backoff_factor: Optional[float]
    error_type: ErrorType


class RetryCountExceeded(Exception):
    pass

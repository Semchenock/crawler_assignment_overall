from abc import ABC
from dataclasses import dataclass
from typing import Optional, Union

from src.async_crawler.enums import ErrorTypes


class BaseError(Exception, ABC):
    def __init__(self, error_type: ErrorTypes):
        self.error_type = error_type

class TransientError(BaseError):
    pass


class PermanentError(BaseError):
    pass


class NetworkError(BaseError):
    pass


class ParseError(BaseError):
    pass


@dataclass
class RetryRule:
    max_retries: Optional[int]
    base_delay: Optional[float]
    backoff_factor: Optional[float]
    error_type: BaseError


class RetryCountExceeded(Exception):
    pass

from src.async_crawler.enums import ErrorTypes
from src.retry_strategy.models import RetryRule, TransientError, NetworkError, PermanentError, ParseError

DEFAULT_RULES = [
    RetryRule(
        error_type=TransientError,
        max_retries=3,
        backoff_factor=2,
        base_delay=2,
    ),
    RetryRule(
        error_type=NetworkError,
        max_retries=4,
        backoff_factor=1.5,
        base_delay=1,
    )
]

ERRORS_TO_STATUS_CODES = {
    TransientError: [503, 429],
    PermanentError: [404, 403, 401],
    NetworkError: [500, 501, 502, 504],
}

ERROR_TYPES_TO_STATUS_CODES = {
    ErrorTypes.NETWORK: [503, 429],
    ErrorTypes.HTTP: [404, 403, 401],
    ErrorTypes.TIMEOUT: [500, 501, 502, 504],
}

STATUS_CODES_TO_ERROR = {
    code: error
    for error, codes in ERRORS_TO_STATUS_CODES.items()
    for code in codes
}

STATUS_CODES_TO_ERROR_TYPES = {
    code: error
    for error, codes in ERROR_TYPES_TO_STATUS_CODES.items()
    for code in codes
}

CUSTOM_ERRORS = (
    TransientError,
    PermanentError,
    NetworkError,
    ParseError)

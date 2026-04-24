import asyncio
import random
from collections import Counter
from typing import Optional, Callable, Awaitable, Any

from .models import RetryRule, RetryCountExceeded
from .constants import DEFAULT_RULES, CUSTOM_ERRORS
from ..error_log.error_log import ErrorLog, LogEntity


class RetryStrategy:
    def __init__(self, max_retries: int = 6, backoff_factor: float = 2.0, base_delay: float = 1.0, retry_on: list[RetryRule] = DEFAULT_RULES, error_log: ErrorLog = ErrorLog()):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.backoff_factor = backoff_factor
        self.retry_on = retry_on
        self.max_jitter = 0.2
        self.error_log = error_log

    def _get_backoff(self, rule: Optional[RetryRule], error_count: int) -> float:
        base_delay = rule.base_delay if rule and rule.base_delay else self.base_delay
        backoff_factor = rule.backoff_factor if rule and rule.backoff_factor else self.backoff_factor\

        backoff = base_delay * pow(backoff_factor, error_count)
        return backoff

    def _get_max_retries(self, rule: Optional[RetryRule]) -> int:
        return rule.max_retries if rule and rule.max_retries else self.max_retries

    async def execute_with_retry(self, coro: Callable[..., Awaitable[Any]], url: str, *args, **kwargs):
        errors_counter = Counter()

        for i in range(1, self.max_retries+1):
            try:
                print(f"try #{i} url: {url}")
                result = await coro(url, attempt=i, *args, **kwargs)
                return result
            except CUSTOM_ERRORS as e:
                print(f"caught {e.__class__.__name__} in url {url}")

                rule = next((r for r in self.retry_on if isinstance(e, r.error_type)), None)

                error_key = e.__class__.__name__
                errors_counter[error_key] += 1
                try_count_by_type = errors_counter.get(error_key, 0)

                max_retries = self._get_max_retries(rule)

                jitter = random.uniform(0, self.max_jitter)
                backoff = self._get_backoff(rule, i)

                log = LogEntity(url=url, error_type=e.__class__.__name__, backoff=backoff, try_count=i, try_count_by_type=try_count_by_type)
                self.error_log.append(log)

                if rule is None:
                    raise

                if try_count_by_type >= max_retries:
                    raise RetryCountExceeded(
                        f"{error_key} exceeded {max_retries} retries"
                    )

                await asyncio.sleep(backoff + jitter)

            except Exception as e:
                print(f"caught UNKNOWN error {e.__class__.__name__} in url {url}")


        raise RetryCountExceeded("Global retry limit exceeded")

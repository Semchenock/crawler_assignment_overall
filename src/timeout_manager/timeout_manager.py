class TimeoutConfig:
    def __init__(
        self,
        connect=2,
        read=5,
        total=10,
        backoff_factor=2.0,
        max_total=60
    ):
        self.connect = connect
        self.read = read
        self.total = total
        self.backoff_factor = backoff_factor
        self.max_total = max_total

    def for_attempt(self, attempt: int):
        factor = self.backoff_factor ** attempt

        return {
            "connect": self.connect * factor,
            "read": self.read * factor,
            "total": min(self.total * factor, self.max_total),
        }
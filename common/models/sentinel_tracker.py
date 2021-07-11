class SentinelTracker:
    def __init__(self, max_expected):
        self.max_expected = max_expected
        self.accum = 0

    def count_and_reached_limit(self):
        self.accum += 1
        return self.accum >= self.max_expected

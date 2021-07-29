class SentinelTracker:
    def __init__(self, max_expected):
        self.max_expected = max_expected
        self.accum = set()

    def count_and_reached_limit(self, emitter_id=None):
        self.accum.add(emitter_id)
        return len(self.accum) >= self.max_expected

    def reset(self):
        self.accum = set()

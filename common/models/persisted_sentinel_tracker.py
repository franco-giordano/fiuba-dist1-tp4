from common.models.persistor import Persistor

class PersistedSentinelTracker:
    def __init__(self, max_expected, filename):
        self.max_expected = max_expected
        self.accum = 0
        self.persistor = Persistor(filename)

        for row in self.persistor.read():
            if row == "SENTINEL\n":
                self.accum += 1

    def count_and_reached_limit(self):
        self.accum += 1
        self.persistor.persist("SENTINEL")
        return self.accum >= self.max_expected

    def reset(self):
        self.persistor.wipe()
        self.accum = 0

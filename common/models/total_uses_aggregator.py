class TotalUsesAggregator:
    def collapse(self, prev_val, player):
        base = prev_val if prev_val is not None else 0
        return base+1

class VictoriesTotalAggregator:
    def collapse(self, prev_val, player):
        base = prev_val if prev_val is not None else [0, 0]
        victory = int(player['winner'])
        return [base[0] + victory, base[1] + 1]

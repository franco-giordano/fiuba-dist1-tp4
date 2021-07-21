class TotalUsesAggregator:
    def collapse(self, all_players):
        return len(all_players)

    def add(self, prev_val, player):
        base = prev_val if prev_val is not None else []
        if player not in base:  # TODO: esto es un O(n) cada vez que llega un player
            base.append(player)
        return base

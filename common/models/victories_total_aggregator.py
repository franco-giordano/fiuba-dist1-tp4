class VictoriesTotalAggregator:
    def add(self, prev_val, player):
        base = prev_val if prev_val is not None else []
        if player not in base:  # TODO: esto es un O(n) cada vez que llega un player
            base.append(player)
        return base
        
    def collapse(self, all_players):
        vic_total_count = [0, 0]

        for player in all_players:
            vic_total_count[0] += int(player['winner'])
            vic_total_count[1] += 1
    
        return vic_total_count

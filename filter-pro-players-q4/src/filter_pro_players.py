class FilterProPlayers:
    def should_pass(self, player):
        return player['rating'] > 2000

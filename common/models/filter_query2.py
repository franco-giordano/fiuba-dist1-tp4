class FilterQuery2:
    def should_pass(self, players):
        assert(len(players) == 2)

        winner = players[0] if players[0]['winner'] else players[1]
        loser = players[0] if not players[0]['winner'] else players[1]

        correct_diff = (winner['rating'] - loser['rating']) / loser['rating'] < -0.3 \
            if loser['rating'] else False
        
        return correct_diff and winner['rating'] > 1000

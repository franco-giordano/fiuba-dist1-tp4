class BatchedFilterLadder:
    def __init__(self, route_1v1, route_team):
        self.route_1v1 = route_1v1
        self.route_team = route_team

    def _add_by_route(self, match, batch_1v1, batch_team):
        if match['ladder'] == 'RM_1v1':
            batch_1v1.append(match)
        elif match['ladder'] == 'RM_TEAM':
            batch_team.append(match)

    def create_filtered_batches(self, batch):
        batch_1v1 = []
        batch_team = []
        while batch:
            match = batch.pop()
            self._add_by_route(match, batch_1v1, batch_team)
        
        return batch_1v1, batch_team

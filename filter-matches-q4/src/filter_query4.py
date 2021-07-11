class FilterQuery4:
    def should_pass(self, match_dict):
        return match_dict['map'] == 'islands'

class FilterQuery3:
    def should_pass(self, match_dict):
        return match_dict['map'] == 'arena' and not match_dict['mirror']

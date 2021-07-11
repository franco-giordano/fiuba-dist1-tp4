class FilterQuery1:
    def should_pass(self, match_dict):
        is_pro = match_dict['average_rating'] > 2000
        is_correct_server = match_dict['server'] in ('koreacentral', 'southeastasia', 'eastus')
        is_long = match_dict['duration'] > 2*3600  # 2 hours in seconds

        return is_pro and is_correct_server and is_long

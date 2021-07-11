class ShardKeyGetter:
    def __init__(self, shards_amount):
        self.CHARS = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        self.shards_amount = shards_amount
    
    def get_key_for_str(self, token):
        first_char = token[0]
        number = self.CHARS.index(first_char)
        return str(number % self.shards_amount)

    def generate_all_shard_keys(self):
        return [str(i) for i in range(self.shards_amount)]

class SpaceSaving:
    def __init__(self, k):
        self.k = k
        self.counter = {}

    def update(self, item):
        if item in self.counter:
            self.counter[item] += 1
        elif len(self.counter) < self.k:
            self.counter[item] = 1
        else:
            evict_item = min(self.counter, key=self.counter.get)
            del self.counter[evict_item]
            self.counter[item] = self.counter[evict_item] + 1

    def query(self):
        return list(self.counter.items())

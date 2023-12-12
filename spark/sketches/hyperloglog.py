import hashlib

class HyperLogLog:
    def __init__(self, b=10):
        self.m = 2 ** b
        self.M = [0] * self.m
        self.b = b

    def _hash(self, item):
        return int(hashlib.sha1(str(item).encode('utf-8')).hexdigest(), 16)

    def _rho(self, w):
        rho = 1
        while w & (1 << (rho - 1)) == 0 and rho <= 64:
            rho += 1
        return rho

    def add(self, item):
        x = self._hash(item)
        j = x & (self.m - 1)
        w = x >> self.b
        self.M[j] = max(self.M[j], self._rho(w))

    def count(self):
        Z = 1.0 / sum([2 ** -Mj for Mj in self.M])
        raw_estimate = self.m ** 2 * Z / 0.7213 / (1 + 1.079 / self.m)
        # There are additional corrections for small and large ranges that could be added
        return int(raw_estimate)

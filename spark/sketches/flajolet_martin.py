import hashlib
import numpy as np

class FlajoletMartinSketch:
    def __init__(self, num_perm=128):
        self.num_perm = num_perm
        self.bit_arrays = [0] * num_perm

    def _hash_function(self, item, seed):
        return bin(int(hashlib.sha256((str(item) + str(seed)).encode('utf-8')).hexdigest(), 16))

    def _rho(self, hashed_value):
        return len(hashed_value) - len(hashed_value.rstrip('0'))

    def add_value(self, value):
        for i in range(self.num_perm):
            hashed_value = self._hash_function(value, i)
            self.bit_arrays[i] = max(self.bit_arrays[i], self._rho(hashed_value))

    def get_sketch(self):
        return self.bit_arrays

    def estimate_count(self):
        estimates = [2 ** rho for rho in self.bit_arrays]
        return int(np.median(estimates))

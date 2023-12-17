import pandas as pd
import hashlib
import numpy as np

class MinHashSketch:
    def __init__(self, num_perm=128):
        self.num_perm = num_perm
        self.min_values = np.full(num_perm, np.inf)
    
    def hash_functions(self, x):
        return [hash(x + str(i)) % 10**10 for i in range(self.num_perm)]
    
    def add_values(self, values):
        for value in values:
            hashes = self.hash_functions(value)
            self.min_values = np.minimum(self.min_values, hashes)
    
    def get_sketch(self):
        return self.min_values
    
    @staticmethod
    def jaccard_similarity(sketch1, sketch2):
        intersection = len(set(sketch1).intersection(sketch2))
        union = len(set(sketch1).union(sketch2))
        return intersection / union if union != 0 else 0
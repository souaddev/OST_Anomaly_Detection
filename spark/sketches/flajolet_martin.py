import hashlib

class FlajoletMartinSketch:
    def __init__(self):
        self.bit_array = [0] * 32  # Using 32 bits for simplicity

    def add(self, item):
        binary_hash = bin(int(hashlib.sha256(str(item).encode('utf-8')).hexdigest(), 16))
        trailing_zeros = len(binary_hash) - len(binary_hash.rstrip('0'))
        index = min(trailing_zeros, 31)  # Cap the index at 31 for our bit array size
        self.bit_array[index] = 1

    def estimate_count(self):
        R = self.bit_array.index(0) if 0 in self.bit_array else 32
        return 2 ** R

import random
import sys

for line in sys.stdin:
    res = random.randint(1, 10000)
    print(res, line.strip(), sep='\t')

import random
import sys

res = []
n = random.randint(1, 5)
counter = 0

for line in sys.stdin:
    res.append(line.split('\t')[1].strip())
    counter += 1
    if counter == n:
        print(','.join(res))
        counter = 0
        res = []
        n = random.randint(1, 5)
      
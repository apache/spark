import sys

for row in sys.stdin:
    print('\t'.join([w + '#' for w in row[:-1].split('\t')]))

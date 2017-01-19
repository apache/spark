import sys

delim = sys.argv[1]

for row in sys.stdin:
    print(delim.join([w + '#' for w in row[:-1].split(delim)]))

import sys

for line in sys.stdin:
    arr = line.strip().split("\t")
    for i in range(len(arr)):
        arr[i] = arr[i] + "#"
    print("\t".join(arr))

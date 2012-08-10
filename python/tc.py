from rdd import SparkContext

sc = SparkContext("local", "PythonWordCount")
e = [(1, 2), (2, 3), (4, 1)]

tc = sc.parallelizePairs(e)

edges = tc.mapPairs(lambda (x, y): (y, x))

oldCount = 0
nextCount = tc.count()

def project(x):
    return (x[1][1], x[1][0])

while nextCount != oldCount:
    oldCount = nextCount
    tc = tc.union(tc.join(edges).mapPairs(project)).distinct()
    nextCount = tc.count()

print "TC has %i edges" % tc.count()
print tc.collect()

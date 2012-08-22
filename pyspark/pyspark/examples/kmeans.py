import sys

from pyspark.context import SparkContext
from numpy import array, sum as np_sum


def parseVector(line):
    return array([float(x) for x in line.split(' ')])


def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist = np_sum((p - centers[i]) ** 2)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print >> sys.stderr, \
            "Usage: PythonKMeans <master> <file> <k> <convergeDist>"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonKMeans")
    lines = sc.textFile(sys.argv[2])
    data = lines.map(parseVector).cache()
    K = int(sys.argv[3])
    convergeDist = float(sys.argv[4])

    kPoints = data.takeSample(False, K, 34)
    tempDist = 1.0

    while tempDist > convergeDist:
        closest = data.map(
            lambda p : (closestPoint(p, kPoints), (p, 1)))
        pointStats = closest.reduceByKey(
            lambda (x1, y1), (x2, y2): (x1 + x2, y1 + y2))
        newPoints = pointStats.map(
            lambda (x, (y, z)): (x, y / z)).collect()

        tempDist = sum(np_sum((kPoints[x] - y) ** 2) for (x, y) in newPoints)

        for (x, y) in newPoints:
            kPoints[x] = y

    print "Final centers: " + str(kPoints)

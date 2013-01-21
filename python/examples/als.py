"""
This example requires numpy (http://www.numpy.org/)
"""
from os.path import realpath
import sys

import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark import SparkContext

LAMBDA = 0.01   # regularization
np.random.seed(42)

def rmse(R, ms, us):
    diff = R - ms * us.T
    return np.sqrt(np.sum(np.power(diff, 2)) / M * U)

def update(i, vec, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]
    XtX = matrix(np.zeros((ff, ff)))
    Xty = np.zeros((ff, 1))

    for j in range(uu):
        v = mat[j, :]
        XtX += v.T * v
        Xty += v.T * ratings[i, j]
    XtX += np.eye(ff, ff) * LAMBDA * uu
    return np.linalg.solve(XtX, Xty)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, \
            "Usage: PythonALS <master> <M> <U> <F> <iters> <slices>"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonALS", pyFiles=[realpath(__file__)])
    M = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    U = int(sys.argv[3]) if len(sys.argv) > 3 else 500
    F = int(sys.argv[4]) if len(sys.argv) > 4 else 10
    ITERATIONS = int(sys.argv[5]) if len(sys.argv) > 5 else 5
    slices = int(sys.argv[6]) if len(sys.argv) > 6 else 2

    print "Running ALS with M=%d, U=%d, F=%d, iters=%d, slices=%d\n" % \
            (M, U, F, ITERATIONS, slices)

    R = matrix(rand(M, F)) * matrix(rand(U, F).T)
    ms = matrix(rand(M ,F))
    us = matrix(rand(U, F))

    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)

    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), slices) \
               .map(lambda x: update(x, msb.value[x, :], usb.value, Rb.value)) \
               .collect()
        ms = matrix(np.array(ms)[:, :, 0])      # collect() returns a list, so array ends up being
                                                # a 3-d array, we take the first 2 dims for the matrix
        msb = sc.broadcast(ms)

        us = sc.parallelize(range(U), slices) \
               .map(lambda x: update(x, usb.value[x, :], msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)

        error = rmse(R, ms, us)
        print "Iteration %d:" % i 
        print "\nRMSE: %5.4f\n" % error

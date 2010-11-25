#!/usr/bin/env python2.6
'''
This is a very simple program to illustrate how to use the qjam framework. It
shows how to:

  * create a pool of workers
  * create a simple DataSet object out of a numpy matrix
  * start a qjam master
  * run a job on the worker pool and retrieve the result

All of the significant pieces are sufficiently commented to help you understand
how to write programs to use qjam.
'''
import os
import sys

import numpy

sys.path.append('..')  # Set up the import path to qjam.
from qjam.dataset import NumpyMatrixDataSet
from qjam.master.master import Master
from qjam.master.remote_worker import RemoteWorker

# sum_dataset is an example module that contains a function that is run on each
# piece of the dataset.
from examples import sum_dataset


def main():
  # Worker hostnames are the commandline options. Each hostname specified on
  # the commandline will be bootstrapped and have a worker started on it.
  if len(sys.argv) < 2:
    print 'usage: %s <hostnames> [<hostname> ...]' % sys.argv[0]
    sys.exit(1)
  hostnames = sys.argv[1:]

  # Start the worker processes on the specified hostnames.
  workers = []
  for i, hostname in enumerate(hostnames):
    worker = RemoteWorker(hostname)
    workers.append(worker)
    print 'Started worker %d on %s' % (i, hostname)

  # Create a numpy matrix.
  matrix = numpy.matrix('1 2 3 4; 5 6 7 8; 9 10 11 12')

  print 'Created matrix:'
  print matrix
  print

  # Create a DataSet object from this matrix.
  #
  # DataSet objects are necessary so the framework can determine how to split
  # up the data into smaller chunks and distribute the chunks to the workers.
  #
  # Here, we set the slice size to 1, so each row of the matrix has the
  # potential to get distributed to a different worker. A larger slice size of
  # two, say, would make the smallest 'work unit' be two rows. In that case,
  # each worker would be given a minimum of two rows of the matrix.
  dataset = NumpyMatrixDataSet(matrix, slice_size=1)

  print 'DataSet contains %d slices.' % len(dataset)
  print

  # Sum the matrix!
  master = Master(workers)
  params = None  # No parameters needed for this job.
  result = master.run(sum_dataset, params, dataset)

  # Print the result.
  print 'Result is: %d' % result


if __name__ == '__main__':
  main()
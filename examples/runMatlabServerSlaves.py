
MATLAB_CMD = '''matlab -nodisplay -nosplash -nodesktop << EOF
cd %(path)s
slaveRun('%(host)s', %(port)d)
exit
EOF
'''
def runSlave(info, workernum):
  '''Starts a slave with info '''
  try:
    cmd = MATLAB_CMD % info
    import os
    #os.system(cmd)

    import subprocess
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    proc.wait()
    output = str(proc.stdout.readlines())

    return 'WORKER %d DONE -----------------\n%s\n\n%s\n\n' % \
      (int(workernum[0]), cmd, output)

  except Exception, e:
    import sys
    import traceback
    exc_type, exc_value, exc_traceback = sys.exc_info()
    output = '%s\n\n%s' % (e, str(traceback.format_exception(exc_type,
      exc_value, exc_traceback)))

    return 'WORKER %d FAILED -----------------\n%s\n\n%s\n\n' % \
      (int(workernum[0]), cmd, output)

# QJAM functions:
mapfunc = runSlave
reduce = lambda x, y: '%s\n%s' % (str(x), str(y))

def main():

  import logging
  import os
  import sys

  # Add parent directory to path.
  # Note that these imports cannot be at the top level because the worker
  # does not have these modules.
  sys.path.append(os.path.join(os.path.dirname(sys.argv[0]), '..'))
  from qjam.master import Master, RemoteWorker
  from qjam.dataset import BaseDataSet
  from qjam import DataSet

  class WorkersDataSet(BaseDataSet):
    '''Workers DataSet.'''
    def __init__(self, workers):
      workers = int(workers)
      BaseDataSet.__init__(self, workers)
      self._workers = workers
    def __len__(self):
      return self._workers

    def chunks(self):
      return self._workers

    def slice(self, index):
      return DataSet([index])


  _fmt = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
  logging.basicConfig(level=logging.INFO, format=_fmt)

  if len(sys.argv) < 3 or len(sys.argv[2].split(':')) > 2:
    print "runs adam framework slaves"
    print "usage: %s <path to fwk> <master> [<worker> ...]" % sys.argv[0]
    exit(1)

  params = {}
  params['path'] = sys.argv[1]
  master_address = sys.argv[2]
  if ':' in master_address:
    host, port = master_address.split(':')
    params['host'] = host
    params['port'] = int(port)
  else:
    params['host'] = master_address
    params['port'] = 12345

  cluster = sys.argv[3:]
  if not cluster:
    cluster = ['localhost']

  mod = sys.modules[__name__]
  master = Master([RemoteWorker(host) for host in cluster])
  dataset = WorkersDataSet(len(cluster))

  print 'Running fwk in %(path)s at %(host)s:%(port)d' % params
  print master.run(mod, params=params, dataset=dataset)


if __name__ == "__main__":
  main()

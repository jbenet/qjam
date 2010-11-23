from nose.tools import *
import json
import os
import subprocess
import sys

import qjam.worker.worker

# utils
from utils import source, encode, decode

# Test modules. These are serialized and sent to the worker to execute.
import constant
import sum_params


class Test_Worker:
  def setup(self):
    _path = qjam.worker.worker.__file__
    _path = _path.replace('.pyc', '.py')
    self._worker = subprocess.Popen(_path,
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    bufsize=0)

  def teardown(self):
    self._worker.kill()

  def read_stderr(self):
    return self._worker.stderr.readline()

  def read_message(self):
    msg_str = self._worker.stdout.readline()
    return json.loads(msg_str.strip())

  def read_error_string(self):
    '''Read a line from stdout, expecting to see an error.

    Returns:
      error string
    '''
    msg = self.read_message()
    assert_true('type' in msg)
    assert_equals('error', msg['type'], 'expecting error message type')
    return msg['error']

  def write_message(self, msg):
    print '\nSending: %s' % json.dumps(msg)
    self._worker.stdin.write('%s\n' % json.dumps(msg))

  def write_command(self, cmd, data):
    data['type'] = cmd
    self.write_message(data)

  def test_process(self):
    assert_not_equals(self._worker.stdin, None)
    assert_not_equals(self._worker.stdout, None)
    assert_not_equals(self._worker.stderr, None)

  def test_bad_message(self):
    self.write_message({'bogus_key': 1234})
    assert_true('missing type' in self.read_error_string())

  def test_bad_type(self):
    self.write_command('bogus_command', {})
    assert_true('unexpected message type' in self.read_error_string())

  def test_incomplete_task(self):
    c = encode(source(constant))
    msg1 = {'module': c,
            'params': ()}
    msg2 = {'module': c,
            'dataset': None}
    msg3 = {'dataset': None,
            'params': ()}

    for msg in (msg1, msg2, msg3):
      self.write_command('task', msg)
      assert_true('missing key' in self.read_error_string())

  def run_task(self, module, params, dataset):
    msg = {'module': encode(source(module)),
           'params': encode(params),
           'dataset': dataset}
    self.write_command('task', msg)

    state_msg = self.read_message()
    assert_true('type' in state_msg)
    assert_equal('state', state_msg['type'])
    assert_true('status' in state_msg)
    # TODO(ms): This assumes worker has all refs.
    assert_equal('running', state_msg['status'])

    result_msg = self.read_message()
    assert_true('result' in result_msg)
    result = decode(result_msg['result'])
    return result

  def test_constant(self):
    result = self.run_task(constant, None, [])
    assert_equals(42, result)

  def test_sum(self):
    params = [1, 2, 3, 6, 7, 9]
    result = self.run_task(sum_params, params, [])
    assert_equals(sum(params), result)

  def test_multiple_tasks(self):
    '''Run multiple tasks on the same worker instance.'''
    self.test_constant()
    self.test_sum()
    self.test_constant()
    self.test_sum()
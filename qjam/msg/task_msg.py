#!/usr/bin/python

import base64
import cPickle as pickle
import json

from base_msg import BaseMsg, encode_msg_field, decode_msg_field

class TaskMsg(BaseMsg):
  def __init__(self, module_object, dataset, params):
    BaseMsg.__init__(self, 'task')
    self.__module = module_object
    self.__dataset = dataset
    self.__params = params

  def json_str(self):
    msg = {'type': self.type(),
           'module':  encode_msg_field(self.__module_src(self.__module)),
           'dataset': encode_msg_field(self.__dataset),
           'params':  encode_msg_field(self.__params)}
    return json.dumps(msg)

  def __module_src(self, module):
    '''Return the source code for a given module object.'''
    filename = module.__file__
    filename = filename.replace('.pyc', '.py')
    with open(filename, 'r') as fh:
      return fh.read()
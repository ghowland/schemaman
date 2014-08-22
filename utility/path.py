"""
File path related functions.
"""


import yaml


# Key is file path, this is to store YAML decoded data, for performance
DATA_CACHE = {}


def LoadYaml(path):
  global DATA_CACHE
  
  if path not in DATA_CACHE:
    DATA_CACHE[path] = yaml.load(open(path))
  
  return DATA_CACHE[path]



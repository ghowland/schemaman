"""
File path related functions.

Standard operations are wrapped here to be shorter and also to provide a place to create consistency between our operations.
"""


import yaml


# Key is file path, this is to store YAML decoded data, for performance
DATA_CACHE = {}


def LoadYaml(path, use_cache=True):
  """Load data from YAML format."""
  global DATA_CACHE
  
  if not use_cache or path not in DATA_CACHE:
    DATA_CACHE[path] = yaml.load(open(path))
  
  return DATA_CACHE[path]


def SaveYaml(path, data):
  """Save data in YAML format."""
  yaml.dump(data, open(path, 'w'))


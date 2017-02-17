"""
File path related functions.

Standard operations are wrapped here to be shorter and also to provide a place to create consistency between our operations.
"""


import yaml
import collections

try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Dumper as Dumper
    from yaml import Loader as Loader


# Key is file path, this is to store YAML decoded data, for performance
DATA_CACHE = {}


class SafeDumper(Dumper):
  """Create a class to handle unicode messiness in YAML dumps."""
  
  def ignore_aliases(self, data):
    """
    A dumper that will never emit aliases.
    """
    return True


# Strings as strings
SafeDumper.add_representer(unicode, yaml.representer.SafeRepresenter.represent_str)

# Unicode as strings
SafeDumper.add_representer(unicode, yaml.representer.SafeRepresenter.represent_unicode)


def LoadYaml(path, use_cache=True):
  """Load data from YAML format."""
  global DATA_CACHE
  
  if not use_cache or path not in DATA_CACHE:
    DATA_CACHE[path] = yaml.load(open(path), Loader=Loader)
  
  return DATA_CACHE[path]


def SaveYaml(path, data):
  """Save data in YAML format."""
  yaml.dump(data, open(path, 'w'), Dumper=SafeDumper)


def LoadYamlFromString(text, default_value=None, strict=False):
  """Loads YAML data from a string
  
  Returns: any data container type storable in YAML, or None
  """
  # None is never a failure, its just the default value
  if text == None:
    return default_value
  
  # If we are not strict, we will handle all exceptions as setting default value
  if not strict:
    try:
      result = yaml.load(text, Loader=Loader)
    
    except Exception, e:
      result = default_value
  
  # Else, we will raise exceptions as normal.  Separate else section so stack trace is from the failure point, and not hidden
  else:
      result = yaml.load(text, Loader=Loader)
  
  return result


def DumpYamlAsString(data, deep_unicode_conversion=False):
  """Wraps dumping YAML, in the way we need it to diff against existing YAML."""
  # Convert all unicode to strings, to get rid of annoying YAML formatting
  if deep_unicode_conversion:
    data = DeepUnicodeToString(data)
  
  # Better than safe_dump, for clarity.
  result = yaml.dump(data, Dumper=SafeDumper)
  
  return result


def DeepUnicodeToString(data):
  """Converts deep data structures into all strings.  No more unicode."""
  if type(data) is unicode:
    return str(data.encode('ascii', 'ignore'))
  if type(data) is list:
    return [DeepUnicodeToString(a) for a in data]
  if type(data) is dict:
    return dict((DeepUnicodeToString(key), DeepUnicodeToString(value)) for key, value in data.iteritems())
  if isinstance(data, collections.OrderedDict):
    return collections.OrderedDict((DeepUnicodeToString(key), DeepUnicodeToString(value)) for key, value in data.iteritems())
  
  return data


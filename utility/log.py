"""
Logging
"""


import logging
import time
import sys


# Default logging level
LOG_LEVEL_DEFAULT = logging.INFO


# Minimum log level we will actually log.  Beneath this we ignore (can ignore DEBUG, WARN, etc)
# LOG_LEVEL_MINIMUM = logging.INFO
LOG_LEVEL_MINIMUM = logging.DEBUG


def Log(text, log_level=None):
  """Send log messages to STDERR, so we can template to STDOUT by default (no output path, easier testing)
  
  Implement with rotating files using Python logging, prepped for that.  De-duping, counting, mini-stack (calling function, lineno) can be added, etc.
  """
  # Dont log this if it is beneath our default level, and it was explicitly passed in.
  if log_level and log_level < LOG_LEVEL_MINIMUM:
    return
  
  elif log_level == None:
    log_level = LOG_LEVEL_DEFAULT
  
  
  timestamp = '[%d-%02d-%02d %02d:%02d:%02d] ' % time.localtime()[:6]
  sys.stderr.write(timestamp + str(text) + '\n')
  sys.stderr.flush()


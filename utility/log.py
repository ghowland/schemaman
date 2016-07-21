"""
Logging
"""


import logging
import time
import sys


# Default logging level
LOG_LEVEL_DEFAULT = logging.INFO


def Log(text, log_level=LOG_LEVEL_DEFAULT):
  """Send log messages to STDERR, so we can template to STDOUT by default (no output path, easier testing)
  
  Implement with rotating files using Python logging, prepped for that.  De-duping, counting, mini-stack (calling function, lineno) can be added, etc.
  """
  timestamp = '[%d-%02d-%02d %02d:%02d:%02d] ' % time.localtime()[:6]
  sys.stderr.write(timestamp + str(text) + '\n')
  sys.stderr.flush()


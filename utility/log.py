"""
Logging
"""

import time
import sys


def log(text):
  """Send log messages to STDERR, so we can template to STDOUT by default (no output path, easier testing)"""
  timestamp = '[%d-%02d-%02d %02d:%02d:%02d] ' % time.localtime()[:6]
  sys.stderr.write(timestamp + str(text) + '\n')
  sys.stderr.flush()


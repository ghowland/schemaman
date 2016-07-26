#!/usr/bin/env python
"""
SchemaMan - Schema Manager.  Cross database schema revision control, data migration and data access.

- Manage changes in the schema versions, allowing publication of schemas with your code that is not bound to a single database
    software package or platform.
- Use as a cross-database method of interacting with data (insert/update/delete/get/filter).
- Migrate data between different databases (same or different database software), using rules to update schema, data, or a
    combination.
- Version Management of all data put into the system (unless skipped)
- Change Management of Version Management commits (done through a series of Action scripts, so highly configurable)

Intended for use on smaller data sets, such as those found in System and Network Operational Configuration Management systems.

Copyright Geoff Howland, 2014.  MIT License.
"""


import sys
import os
import getopt
import pprint

# SchemaMan modules
import utility
from utility.log import Log
from utility.error import *
from utility.path import *

import action


def Main(args=None):
  if not args:
    args = []

  
  long_options = ['dir=', 'verbose', 'help', 'yes']
  
  try:
    (options, args) = getopt.getopt(args, '?hvyd:', long_options)
  except getopt.GetoptError, e:
    Usage(e)
  
  # Dictionary of command options, with defaults
  command_options = {}
  command_options['verbose'] = False
  command_options['always_yes'] = False
  
  
  # Process out CLI options
  for (option, value) in options:
    # Help
    if option in ('-h', '-?', '--help'):
      Usage()
    
    # Verbose output information
    elif option in ('-v', '--verbose'):
      command_options['verbose'] = True
    
    # Always answer Yes to prompts
    elif option in ('-y', '--yes'):
      command_options['always_yes'] = True
    
    # Invalid option
    else:
      Usage('Unknown option: %s' % option)


  # Store the command options for our logging
  utility.log.RUN_OPTIONS = command_options
  
  
  # Ensure we at least have one spec file
  if len(args) < 1:
    Usage('No action specified')
  

  #try:
  if 1:
    action.process.ProcessAction(args[0], args[1:], command_options)
  
  
  #NOTE(g): Catch all exceptions, and return in properly formatted output
  #TODO(g): Implement stack trace in Exception handling so we dont lose where this
  #   exception came from, and can then wrap all runs and still get useful
  #   debugging information
  #except Exception, e:
  else:
    Error({'exception':str(e)}, command_options)


if __name__ == '__main__':
  #NOTE(g): Fixing the path here.  If you're calling this as a module, you have to 
  #   fix the utility/handlers module import problem yourself.
  sys.path.append(os.path.dirname(sys.argv[0]))
  
  Main(sys.argv[1:])

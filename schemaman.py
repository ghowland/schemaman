#!/usr/bin/env python
"""
SchemaMan - Schema Manager.  Cross database schema revision control, data migration and data access.

- Manage changes in the schema versions, allowing publication of schemas with your code that is not bound to a single database
    software package or platform.
- Use as a cross-database method of interacting with data (insert/update/delete/get/filter).
- Migrate data between different databases (same or different database software), using rules to update schema, data, or a
    combination.

Intended for use on smaller data sets, such as those found in System and Network Operational Configuration Management systems.

Copyright Geoff Howland, 2014.  MIT License.

"""


import sys
import os
import getopt

import utility
from utility.log import Log
from utility.error import Error
from utility.path import *

# All the datasource stuff is wrapped under this
import datasource


def ProcessAction(action, action_args, command_options):
  """Process the specified action, by it's action arguments.  Using command options."""
  
  # If Action is info
  if action == 'info':
    if action_args:
      Usage('info action does not take any arguments: %s' % action_args)
  
  # Else, Initialize a directory to be a SchemaMan location
  elif action == 'init':
    if len(action_args) == 0:
      Usage('"init" action requires arguments: path of schema definition')
    elif os.path.isdir(action_args[0]):
      Usage('"init" action requires arguments: %s: Is not a directory' % action_args[0])
    
    schema_path = action_args[0]
    
    Log('Initializing Schema Definition Path: %s' % schema_path)
    
    # Check to see if we havent already created this schema definition.  We don't allow init twice, let them clean it up.
    pass
  
    # Create the schema files.  Should these get corrected names?
  
  # Else, if Action prefix is Schema
  elif action == 'schema':
    if len(action_args) == 0:
      Usage('"schema" action requires arguments: create, export, migrate')
    elif action_args[0] == 'create':
      result = datasource.CreateSchema()
    
    elif action_args[0] == 'export':
      result = datasource.ExportSchema()
    
    elif action_args[0] == 'migrate':
      # Export from one, and import to another, in one step
      source_result = datasource.ExportSchema()
      target_result = datasource.UpdateSchema(source_result)
    
    # ERROR
    else:
      Usage('Unknown Schema action: %s' % action)
  
  
  # Else, if Action prefix is Data
  elif action == 'data':
    if len(action_args) == 0:
      Usage('"data" action requires arguments: export, import')
    elif action_args[0] == 'export':
      result = datasource.ExportData()
    
    elif action_args[0] == 'import':
      result = datasource.ImportData()
    
    # ERROR
    else:
      Usage('Unknown Data action: %s' % action)
  
  # Put
  elif action == 'put':
    result = datasource.Put()
  
  # Get
  elif action == 'get':
    result = datasource.Get()
  
  # Filter
  elif action == 'filter':
    result = datasource.Filter()
  
  # Delete
  elif action == 'delete':
    result = datasource.Delete()
  
  # ERROR
  else:
    Usage('Unknown action: %s' % action)


def Usage(error=None):
  """Print usage information, any errors, and exit.  

  If errors, exit code = 1, otherwise 0.
  """
  output = ''
  
  if error:
    output += '\nerror: %s\n' % error
    exit_code = 1
  else:
    exit_code = 0
  
  output += '\n'
  output += 'usage: %s [options] action <action_args>' % os.path.basename(sys.argv[0])
  output += '\n'
  output += 'Schema Actions:\n'
  output += '\n'
  output += '  info                                       Print info on current schema directory\n'
  output += '  init <path>                                Initialize a path for new schemas\n'
  output += '  schema create <schema>                     Create a schema interactively\n'
  output += '  schema export <schema> <source>            Export a database schema from a source\n'
  output += '  schema update <schema> <source> <target>   Migrate schema/data from source to target\n'
  output += '  data export <schema> <source>              Export all the data from the schema/source\n'
  output += '  data import <schema> <source>              Import data into the schema/source\n'
  output += '\n'
  output += 'Data Actions:\n'
  output += '\n'
  output += '  put <schema> <source> <json>        Put JSON data into a Schema instance\n'
  output += '  get <schema> <source> <json>        Get Schema instance records from JSON keys\n'
  output += '  filter <schema> <source> <json>     Filter Schema instance records\n'
  output += '  delete <schema> <source> <json>     Delete records from Schema instance\n'
  output += '\n'
  output += 'Options:\n'
  output += '\n'
  output += '  -d <path>, --dir=<path>             Directory for SchemaMan data/conf/schemas\n'
  output += '                                          (Default is current working directory)\n'
  output += '  -y, --yes                           Answer Yes to all prompts\n'
  output += '\n'
  output += '  -h, -?, --help                      This usage information\n'
  output += '  -v, --verbose                       Verbose output\n'
  output += '\n'
  
  
  # STDOUT - Non-error exit
  if exit_code == 0:
    sys.stdout.write(output)
  # STDERR - Failure exit
  else:
    sys.stderr.write(output)
  
  sys.exit(exit_code)


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
    ProcessAction(args[0], args[1:], command_options)
    pass
  
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

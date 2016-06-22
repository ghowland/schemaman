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
import pprint

import utility
from utility.log import Log
from utility.error import Error
from utility.path import *


# All the datasource stuff is wrapped under this
import datasource


# Mode for directories we create.
MODE_DIRECTORY = 0755


def ProcessAction(action, action_args, command_options):
  """Process the specified action, by it's action arguments.  Using command options."""
  
  # If Action is info
  if action == 'info':
    if len(action_args) == 0:
      Usage('"init" action requires 1 argument: path schema definition YAML')
    elif not os.path.isfile(action_args[0]):
      Usage('"init" action requires arguments: %s: Is not a file' % action_args[0])
    
    schema_path = action_args[0]
    
    data = LoadYaml(schema_path)
    
    print '\nSchema Specification:\n'
    pprint.pprint(data)
    print
  
  
  # Else, Initialize a directory to be a SchemaMan location
  elif action == 'init':
    if len(action_args) == 0:
      Usage('"init" action requires 1 argument: directory to store schema definition inside of')
    elif not os.path.isdir(action_args[0]):
      Usage('"init" action requires arguments: %s: Is not a directory' % action_args[0])
    
    schema_dir = action_args[0]
    
    Log('Initializing Schema Definition Directory: %s' % schema_dir)
    
    # Collect the initialization data from the user
    data = utility.interactive_input.CollectInitializationDataFromInput()
    
    schema_path = '%s/%s.yaml' % (schema_dir, data['alias'])
    
    # Check to see if we havent already created this schema definition.  We don't allow init twice, let them clean it up.
    if os.path.exists(schema_path):
      Error('The file you requested to initialize already exists, choose another Schema Alias: %s' % schema_path)
  
    # Create the location for our schema record paths.  This is the specific data source record schema, whereas the schema_path is the definition for the data set
    schema_record_path = '%s/schema/%s.yaml' % (schema_dir, data['alias'])
    
    
    # We need to create this directory, if it doesnt exist
    schema_record_path_dir = os.path.dirname(schema_record_path)
    if not os.path.isdir(schema_record_path_dir):
      os.makedirs(schema_record_path_dir, mode=MODE_DIRECTORY)
    
    
    # If we dont have this file yet, create it, so we can write into it
    if not os.path.isfile(schema_record_path):
      with open(schema_record_path, 'w') as fp:
        # Write an empty dictionary for YAML
        fp.write('{}\n')
    
    
    # Format the data into the expandable schema format (we accept lots more data, but on init we just want 1 set of data)
    formatted_data = {
      'alias': data['alias'],
      'name': data['name'],
      'owner_user': data['owner_user'],
      'owner_group': data['owner_group'],
      'datasource': {
        'database': data['database_name'],
        'user': data['database_user'],
        'password_path': data['database_password_path'],
        'master_host_id': 1,
        'hosts': [
          {
            'id': 1,
            'host': data['database_host'],
            'port': data['database_port'],
            'type': data['database_type'],
          },
        ],
      },
      'schema_paths': [schema_record_path],
      'value_type_path': 'data/schema/value_types/standard.yaml',
    }
    
    # Create the schema files.  Should these get corrected names?  It's always schema.yaml in the schema definition directory.  Makes it standardized.  Keep it simpler.  Or use the actual YAML path...
    schema_data = {data['alias']: formatted_data}
    
    SaveYaml(schema_path, schema_data)
    
    Log('Initialized new schema path: %s' % schema_path)
  
  
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



def Error(error, exit_code=1):
  """Error and exit."""
  output = ''
  
  output += '\nerror: %s\n' % error
  
  sys.stdout.write(output)
  
  sys.exit(exit_code)


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

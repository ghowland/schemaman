"""
Error Reporting
"""


import sys
import os


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
  output += 'usage: %s [options] action <action_args>\n' % os.path.basename(sys.argv[0])
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
  output += 'Primary Data Actions:\n'
  output += '\n'
  output += '  put <schema> <source> <json>        Put JSON data into a Schema instance\n'
  output += '  get <schema> <source> <json>        Get Schema instance records from JSON keys\n'
  output += '  filter <schema> <source> <json>     Filter Schema instance records\n'
  output += '  delete <schema> <source> <json>     Delete records from Schema instance\n'
  output += '\n'
  output += 'Additional Data Actions:\n'
  output += '\n'
  output += '  action config version_change_management <target_schema>  Configure Version and Change Management\n'
  output += '  action populate schema_into_db <target_schema>           Populate Schema Into DB\n'
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

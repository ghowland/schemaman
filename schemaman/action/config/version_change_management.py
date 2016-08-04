"""
Actions: Config: Version and Change Management

Configure all the version and change management 
"""



# SchemaMan libraries
import schemaman.utility as utility
from schemaman.utility.interactive_input import *
import schemaman.datasource as datasource


# This action's command on the CLI and also in the connection_data.actions dict as a key for our data
ACTION = 'config__version_change_management'


def Action(connection_data, action_input_args):
  """Perform action: Configure Version and Change Management"""
  print 'Configure Version and Change Management'
  
  data = CollectData(connection_data, action_input_args)
  
  # If we got invalid data or aborted, quit this action
  if data == None:
    return '\nAborting configuration...'
  
  return 'Configured Version Management'


def CollectData(connection_data, action_input_args):
  """Collect data that we need, if we dont already have it stored in our connection_data.actions dict"""
  data = {}
  
  # If we have data in our connection_data, use it
  if ACTION in connection_data['actions']:
    #TODO(g): Validate that this data is correct
    data = connection_data['actions'][ACTION]
  
    print '\nThese are the tables you specified:'
    print '  - Version: Working: %s' % data['table_version_working']
    print '  - Version: Change List: %s' % data['table_version_change_list']
    print '  - Version: Commit: %s' % data['table_version_commit']
    print
    print 'Is this correct?  (Enter Yes to use these values, anything else to enter new values)'
    
    confirmation = ReadLine('$ ')
    
    # Use the saved data, otherwise collect new data
    if confirmation.lower() == 'yes':
      return data
    else:
      data = {}
  
  
  # Get the Database table name
  intro_string = 'Name of table to put the Version Working changes in.'
  GetInputField(data, 'table_version_working', 'Version Working Table', 'working table', intro_string, force_strip=True)
  
  # Get the Table table name
  intro_string = 'Name of table to put the Version Pending Change Lists in.'
  GetInputField(data, 'table_version_change_list', 'Version Pending Change List Table', 'change list table', intro_string, force_strip=True)
  
  # Get the Field table name
  intro_string = 'Name of table to put the Version Committed changes in.'
  GetInputField(data, 'table_version_commit', 'Version Commit Table', 'committed table', intro_string, force_strip=True)

  
  print '\nThese are the tables you specified:'
  print '  - Version: Working: %s' % data['table_version_working']
  print '  - Version: Change List: %s' % data['table_version_change_list']
  print '  - Version: Commit: %s' % data['table_version_commit']
  print
  print 'Is this correct?  (Enter Save or this action will abort)'
  
  confirmation = ReadLine('$ ')
  
  # Aborting
  if confirmation.lower() not in ('save',):
    return None
  
  
  # Save our connection data with the updated information
  if confirmation.lower() == 'save':
    connection_data['actions'][ACTION] = data
    
    datasource.SaveConnectionSpec(connection_data)
  
  
  return data
  
  
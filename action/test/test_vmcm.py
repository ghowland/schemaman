"""
Actions: Test: Test VMCM (Version and Change Managament)

Import a schema into a set of database tables:  database, table, field
"""



# SchemaMan libraries
import utility
from utility.interactive_input import *
import datasource


# This action's command on the CLI and also in the connection_data.actions dict as a key for our data
ACTION = 'test__test_vmcm'


def Action(connection_data, action_input_args):
  """Perform action: Test VMCM (Version and Change Managament)"""
  print 'Test VMCM (Version and Change Managament)'
  
  # Create working Request object
  request = datasource.Request(connection_data, 'testuser', 'testauth')
  
  # Determine table to operate on
  table = 'owner_group'
  
  record_id = 1
  
  # Get what versions are already available
  versions_available = datasource.RecordVersionsAvailable(request, table, record_id)
  
  # Get original data
  record = datasource.Get(request, table, record_id)
  
  # Make a change
  record['name'] = '%s!' % record['name']
  
  # Save the change un-commited (as a version)
  datasource.SetVersion(request, table, record_id, record)
  
  # Get data again (with VM changes applied)
  record_again = datasource.Get(request, table, record_id)
  
  # Get HEAD data (without VM changed applied)
  record_head = datasource.Get(request, table, record_id)
  
  # Abandon working versions of data
  AbandonWorkingVersion(request, table, record_id)
  
  # Get data again (with VM changed applied, but no change)
  record_again_again = datasource.Get(request, table, record_id)
  
  # Make a change again
  record['name'] = '%s!' % record['name']
  
  # Save the change un-commited (as a version)
  datasource.SetVersion(request, table, record_id, record)
  
  # Get again, see change
  record_again_again_again = datasource.Get(request, table, record_id)
  
  # Commit change
  datasource.CommitWorkingVersion(request, table, record_id)
  
  # Get HEAD data, see change
  record_head_again = datasource.Get(request, table, record_id)
  
  # List versions and see where our new version made that change
  versions_available_again = datasource.RecordVersionsAvailable(request, table, record_id)
  

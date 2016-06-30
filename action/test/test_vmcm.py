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
  
  # Determine tables to operate on
  
  # Get original data
  
  # Make a change
  
  # Get data again (with VM changes applied)
  
  # Get HEAD data (without VM changed applied)
  
  # Abort versions of data
  
  # Get data again (with VM changed applied, but no change)
  
  # Make a change again
  
  # Get again, see change
  
  # Commit change
  
  # Get HEAD data, see change
  
  # List versions and see where our new version made that change
  

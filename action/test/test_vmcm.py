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
  
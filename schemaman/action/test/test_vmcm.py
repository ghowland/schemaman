"""
Actions: Test: Test VMCM (Version and Change Managament)

Import a schema into a set of database tables:  database, table, field
"""


# SchemaMan libraries
import schemaman.utility as utility
from schemaman.utility.log import Log
from schemaman.utility.interactive_input import *
import schemaman.datasource as datasource


# This action's command on the CLI and also in the connection_data.actions dict as a key for our data
ACTION = 'test__test_vmcm'


def Action(connection_data, action_input_args):
  """Perform action: Test VMCM (Version and Change Managament)"""
  print 'Test VMCM (Version and Change Managament)'
  
  # Create working Request object
  request = datasource.Request(connection_data, 'ghowland', 'testauth')
  
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
  Log('Set initial changed record in version_working\n\n')
  datasource.SetVersion(request, table, record)
  
  # Get data again (with VM changes applied)
  record_again = datasource.Get(request, table, record_id)
  
  ReadLine('1: Pause for initial set version, type enter to continue: ')
  
  # Get HEAD data (without VM changed applied)
  record_head = datasource.Get(request, table, record_id, use_working_version=False)
  
  # Abandon working versions of data
  Log('Abandon Working version\n\n')
  was_abandoned = datasource.AbandonWorkingVersion(request, table, record_id)
  
  ReadLine('2: Pause for initial set version is abandonded, type enter to continue: ')
  
  # Get data again (with VM changed applied, but no change)
  record_again_again = datasource.Get(request, table, record_id)
  
  # We already made this change once, but if you forgot, this was it:
  # record['name'] = '%s!' % record['name']
  
  # Save the change un-commited (as a version)
  Log('Set second changed record in version_working\n\n')
  datasource.SetVersion(request, table, record)
  
  ReadLine('3: Pause for second set version, type enter to continue: ')
  
  # Get again, see change
  record_again_again_again = datasource.Get(request, table, record_id)
  
  # Commit change
  Log('Commit working version\n\n')
  datasource.CommitWorkingVersion(request)
  
  # Get HEAD data, see change
  record_head_again = datasource.Get(request, table, record_id, use_working_version=False)
  
  # List versions and see where our new version made that change
  versions_available_again = datasource.RecordVersionsAvailable(request, table, record_id)
  

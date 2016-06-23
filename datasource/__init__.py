"""
Datasource Module
"""


# Schema Datasource Utility functions
import utility


# Data Source Type handlers
import mysql


# ----  This needs to be transactional, and user/request based, for the DB connection pooling ------ #
#     Pass in a request number, which will differentiate different user requests.
#     


def DetermineHandlerModule(connection_data, server_id=None):
  """Returns the handler module, which can handle these requests."""
  # If we didnt have a server_id specified, use the master_server_id
  if server_id == None:
    server_id = connection_data['master_server_id']
  
  # Find the master host, which we will assume we are connecting to for now
  #TODO(g): Specify which host in this Set
  found_host = None
  for host_data in connection_data['datasource']['servers']:
    if host_data['id'] == server_id:
      found_host = host_data
      break
  
  if not found_host:
    raise Exception('Could not find server ID in list of servers: %s' % server_id)
  
  
  # Test the host we found for it's connection type
  if found_host['type'] == 'mysql_56':
    return mysql
  
  
  raise Exception('Unknown Data Source Type: %s' % connection_data['type'])


def CreateSchema(connection_data, schema):
  """Create a schema, based on a spec"""
  handler = DetermineHandlerModule(connection_data)
  
  handler.CreateSchema(connection_data, schema)


def ExtractSchema(connection_data):
  """Export a schema, based on a spec, or everything"""
  handler = DetermineHandlerModule(connection_data)
  
  handler.ExtractSchema(connection_data, path)


def ExportSchema(connection_data, path):
  """Export a schema, based on a spec, or everything"""
  handler = DetermineHandlerModule(connection_data)
  
  handler.ExportSchema(connection_data, path)


def UpdateSchema(connection_data, schema):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  handler = DetermineHandlerModule(connection_data)
  
  handler.UpdateSchema(connection_data, schema)


def ExportData(connection_data, path):
  """Export/dump data from this datasource, based on spec, or everything"""
  handler = DetermineHandlerModule(connection_data)
  
  handler.ExportData(connection_data, path)


def ImportData(drop_first=False, transaction=False):
  """Import/load data to this datasource, based on spec, or everything.
  
  Args:
    drop_first: boolean, optional: If true, all data is dropped/deleted before
        the import occurs, otherwise it is an update.  Defaults to false to
        preserve data.
    transaction: boolean, optional: If true, import is done as a single
        transaction.  Defaults to False to avoid extra memory and slowness.
  
  Returns: None
  """
  handler = DetermineHandlerModule(connection_data)
  
  handler.ImportData(connection_data, path)


def Put(connection_data, data):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  """
  handler = DetermineHandlerModule(connection_data)
  
  handler.Put(connection_data, data)


def Get(schema, source, data):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    schema: dict, ...
    source: string, ...
    data: dict, ...
  
  Returns: dict, single record key/values
  """
  handler = DetermineHandlerModule(connection_data)
  
  handler.Get(connection_data, data)


def Filter():
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  handler = DetermineHandlerModule(connection_data)
  
  handler.Filter(connection_data, data)


def Delete():
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  handler = DetermineHandlerModule(connection_data)
  
  handler.Delete(connection_data, data)


def DeleteFilter():
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  handler = DetermineHandlerModule(connection_data)
  
  handler.DeleteFilter(connection_data, data)


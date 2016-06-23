"""
Datasource Module
"""

import threading


# Schema Datasource Utility functions
import utility


# Data Source Type handlers
import mysql


# Every time we start, we use our initial global request counter, and on the beginning of a request, we increment it, so that we can keep transactions together if they use the same request number
GLOBAL_REQUEST_COUNTER = 1
GLOBAL_REQUEST_COUNTER_LOCK = threading.Lock()


# ----  This needs to be transactional, and user/request based, for the DB connection pooling ------ #
#     Pass in a request number, which will differentiate different user requests.
#     


def DetermineHandlerModule(connection_data, request_number, server_id=None):
  """Returns the handler module, which can handle these requests."""
  global GLOBAL_REQUEST_COUNTER
  
  # Increment the request_number, if we dont already have one
  if request_number == None:
    # Thread safe
    try:
      GLOBAL_REQUEST_COUNTER_LOCK.acquire()
      
      request_number = GLOBAL_REQUEST_COUNTER
      GLOBAL_REQUEST_COUNTER += 1
    
    finally:
      GLOBAL_REQUEST_COUNTER_LOCK.release()
  
  
  # If we didnt have a server_id specified, use the master_server_id
  if server_id == None:
    server_id = connection_data['datasource']['master_server_id']
  
  # Find the master host, which we will assume we are connecting to for now
  found_server = None
  for server_data in connection_data['datasource']['servers']:
    if server_data['id'] == server_id:
      found_server = server_data
      break
  
  
  if not found_server:
    raise Exception('Could not find server ID in list of servers: %s' % server_id)
  
  
  # Test the host we found for it's connection type
  if found_server['type'] == 'mysql_56':
    return (mysql, request_number)
  
  
  raise Exception('Unknown Data Source Type: %s' % connection_data['type'])


def TestConnection(connection_data, request_number=None):
  """Connect to the datasource's database and ensure we can read from it."""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.TestConnection(connection_data, request_number)


def CreateSchema(connection_data, schema, request_number=None):
  """Create a schema, based on a spec"""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.CreateSchema(connection_data, schema)


def ExtractSchema(connection_data, request_number=None):
  """Export a schema, based on a spec, or everything"""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.ExtractSchema(connection_data, path)


def ExportSchema(connection_data, path, request_number=None):
  """Export a schema, based on a spec, or everything"""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.ExportSchema(connection_data, path)


def UpdateSchema(connection_data, schema, request_number=None):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.UpdateSchema(connection_data, schema)


def ExportData(connection_data, path, request_number=None):
  """Export/dump data from this datasource, based on spec, or everything"""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.ExportData(connection_data, path)


def ImportData(drop_first=False, transaction=False, request_number=None):
  """Import/load data to this datasource, based on spec, or everything.
  
  Args:
    drop_first: boolean, optional: If true, all data is dropped/deleted before
        the import occurs, otherwise it is an update.  Defaults to false to
        preserve data.
    transaction: boolean, optional: If true, import is done as a single
        transaction.  Defaults to False to avoid extra memory and slowness.
  
  Returns: None
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.ImportData(connection_data, path)


def Put(connection_data, data, request_number=None):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.Put(connection_data, data)


def Get(schema, source, data, request_number=None):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    schema: dict, ...
    source: string, ...
    data: dict, ...
  
  Returns: dict, single record key/values
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.Get(connection_data, data)


def Filter(connection_data, request_number=None):
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.Filter(connection_data, data)


def Delete(connection_data, request_number=None):
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.Delete(connection_data, data)


def DeleteFilter(connection_data, request_number=None):
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.DeleteFilter(connection_data, data)


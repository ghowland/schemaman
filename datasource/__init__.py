"""
Datasource Module
"""

import os
import threading


from utility.error import *
from utility.path import *

# Schema Datasource Utility functions
import tools

# Data Source Type handlers
import mysql


# Every time we start, we use our initial global request counter, and on the beginning of a request, we increment it, so that we can keep transactions together if they use the same request number
GLOBAL_REQUEST_COUNTER = 1
GLOBAL_REQUEST_COUNTER_LOCK = threading.Lock()


# ----  This needs to be transactional, and user/request based, for the DB connection pooling ------ #
#     Pass in a request number, which will differentiate different user requests.
#     


class Request:
  """Contains request information, and can close transactions due to scope GC collections."""
  
  def __init__(self, connection_data, request_number=None, server_id=None):
    self.connection_data = connection_data
    
    # Ensure we have a request number
    if not request_number:
      self.request_number = GetRequestNumber()
    
    else:
      self.request_number = request_number
    
    
    self.server_id
  
  
  def __del__(self):
    """Going out of scope, ensure we release any connections that we have."""
    ReleaseConnections(connection_data, request_number)


def ReleaseConnections(connection_data, request_number):
  """Release any connections we have open and tied to this request_number (wont close them)"""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.ReleaseConnections(connection_data, request_number)


def LoadConnectionSpec(path):
  """Load the connection specification."""
  if not os.path.isfile(path):
    Error('Connection path specified does not exist: %s' % path)
  
  try:
    data = LoadYaml(path)
    
    # Save our path, we will remove this if we save it
    data['__path'] = path
    
  except Exception, e:
    Error('Could not load connection spec YAML: %s: %s' % (path, e))
  
  return data


def SaveConnectionSpec(connection_data):
  """Load the connection specification."""
  try:
    data = dict(connection_data)
    path = data['__path']
    del data['__path']
    
    SaveYaml(path, data)
    
  except Exception, e:
    Error('Could not save connection spec YAML: %s: %s' % (path, e))
  
  return data


def GetRequestNumber():
  """Returns an int, the next available request number.
  
  This number can be used with any number of data sets simultaneously, as it is globally unique, and
  each database connection will reside in it's own server's pool, so any number of databases
  can be queried with the same request_number.
  """
  global GLOBAL_REQUEST_COUNTER
  
  # Thread safe
  try:
    GLOBAL_REQUEST_COUNTER_LOCK.acquire()
    
    request_number = GLOBAL_REQUEST_COUNTER
    GLOBAL_REQUEST_COUNTER += 1
  
  finally:
    GLOBAL_REQUEST_COUNTER_LOCK.release()
  
  return GLOBAL_REQUEST_COUNTER


def DetermineHandlerModule(connection_data, request_number, server_id=None):
  """Returns the handler module, which can handle these requests."""
  # Increment the request_number, if we dont already have one
  if request_number == None:
    request_number = GetRequestNumber()
  
  
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
  
  result = handler.TestConnection(connection_data, request_number)
  
  return result


def CreateSchema(connection_data, schema, request_number=None):
  """Create a schema, based on a spec"""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  handler.CreateSchema(connection_data, schema)


def ExtractSchema(connection_data, request_number=None):
  """Export a schema, based on a spec, or everything"""
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  result = handler.ExtractSchema(connection_data, request_number)
  
  return result


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


def Set(connection_data, table, data, request_number=None):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  result = handler.Set(connection_data, table, data, request_number)
  
  return result


def Get(connection_data, table, record_id, request_number=None):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    connection_data: dict, Connection Data for a Schema Data Set
    table: string, Record table name
    data: dict, ...
  
  Returns: dict, single record key/values
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  result = handler.Get(connection_data, table, record_id, request_number)
  
  return result


def Filter(connection_data, table, data, request_number):
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  (handler, request_number) = DetermineHandlerModule(connection_data, request_number)
  
  result = handler.Filter(connection_data, table, data, request_number)
  
  return result


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


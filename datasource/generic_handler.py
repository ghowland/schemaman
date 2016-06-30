"""Data source handler.  Generic handler for all datasource types."""


import os
import threading


from utility.error import *
from utility.path import *

# Schema Datasource functions
from request import Request
import tools

# Data Source Type handlers
import mysql


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


def DetermineHandlerModule(request):
  """Returns the handler module, which can handle these requests."""
  # If we didnt have a server_id specified, use the master_server_id
  if request.server_id == None:
    server_id = request.request['datasource']['master_server_id']
  else:
    server_id = request.server_id
  
  # Find the master host, which we will assume we are connecting to for now
  found_server = None
  for server_data in request.request['datasource']['servers']:
    if server_data['id'] == server_id:
      found_server = server_data
      break
  
  
  if not found_server:
    raise Exception('Could not find server ID in list of servers: %s' % server_id)
  
  
  # Test the host we found for it's connection type
  if found_server['type'] == 'mysql_56':
    handler = mysql
  
  else:
    raise Exception('Unknown Data Source Type: %s' % request.request['type'])
  
  
  # Add this handler to the request (if it doesnt have it), so it knows it needs to release these connections on Request object destruction
  request.AddHandler(handler)
  
  
  return handler


def TestConnection(request):
  """Connect to the datasource's database and ensure we can read from it."""
  handler = DetermineHandlerModule(request)
  
  result = handler.TestConnection(request)
  
  return result


def CreateSchema(request, schema):
  """Create a schema, based on a spec"""
  handler = DetermineHandlerModule(request)
  
  handler.CreateSchema(request, schema)


def ExtractSchema(request):
  """Export a schema, based on a spec, or everything"""
  handler = DetermineHandlerModule(request)
  
  result = handler.ExtractSchema(request)
  
  return result


def ExportSchema(request, path):
  """Export a schema, based on a spec, or everything"""
  handler.ExportSchema(request, path)


def UpdateSchema(request, schema):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  handler = DetermineHandlerModule(request)
  
  handler.UpdateSchema(request, schema)


def ExportData(request, path):
  """Export/dump data from this datasource, based on spec, or everything"""
  handler = DetermineHandlerModule(request)
  
  handler.ExportData(request, path)


def ImportData(request, drop_first=False, transaction=False):
  """Import/load data to this datasource, based on spec, or everything.
  
  Args:
    drop_first: boolean, optional: If true, all data is dropped/deleted before
        the import occurs, otherwise it is an update.  Defaults to false to
        preserve data.
    transaction: boolean, optional: If true, import is done as a single
        transaction.  Defaults to False to avoid extra memory and slowness.
  
  Returns: None
  """
  handler = DetermineHandlerModule(request)
  
  handler.ImportData(request, path)


def RecordVersionsAvailable(request, table, record_id, username):
  """List all of the historical and currently available versions available for this record."""
  handler = DetermineHandlerModule(request)
  
  pass#...Do this...


def Set(request, table, data, commit_version=False, version_number=None):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record to set into table
    request_number: int (default None), if not None, this is a known request number, which allows us to perform
        transactions, and re-use the same DB connections
    commit_version: boolean (default False), if True, this will attempt to Commit the Version data after it has
        been stored in version_change as a single record update, without any additional VMCM actions
    version_number: int (default None), if an int, this is the version number in the version_change table to write to,
        if None this will create a new version_working entry
  
  Returns: int or None, if creating a new record this returns the newly created record primary key (ex: `id`), otherwise None
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.Set(request, table, data)
  
  return result


def Get(request, table, record_id, version_number=None, use_working_version=True):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, primary key (ex: `id`) of the record in this table.  Use Filter() to use other field values
    version_number: int (default None), if an int, this is the version number in the version_change or version_commit
        tables.  version_change is scanned before version_commit, as these are more likely to be requested.
    use_working_version: boolean (default True), if True and version_number==None this will also look at any
        version_working data and return it instead the head table data, if it exists for this user.
    
  Returns: dict, single record key/values
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.Get(request, table, record_id, request_number)
  
  return result


def Filter(request, table, data, version_number=None):
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  handler = DetermineHandlerModule(request)

  result = handler.Filter(request, table, data)
  
  return result


def Delete(request, version_number=None):
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  handler = DetermineHandlerModule(request)
  
  return handler.Delete(request, data)


def DeleteFilter(request, version_number=None):
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  handler = DetermineHandlerModule(request)
  
  return handler.DeleteFilter(request, data)




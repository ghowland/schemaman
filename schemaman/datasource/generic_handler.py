"""Data source handler.  Generic handler for all datasource types."""


import os
import threading

from schemaman.utility.error import *
from schemaman.utility.path import *

# Schema Datasource functions
from request import Request
import tools

print tools

class RecordNotFound(Exception):
  """An expected record was not found."""


class VersionNotFound(Exception):
  """Version not found.  In version_* tables."""


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
    server_id = request.connection_data['datasource']['master_server_id']
  else:
    server_id = request.server_id
  
  # Find the master host, which we will assume we are connecting to for now
  found_server = None
  for server_data in request.connection_data['datasource']['servers']:
    if server_data['id'] == server_id:
      found_server = server_data
      break
  
  
  if not found_server:
    raise Exception('Could not find server ID in list of servers: %s' % server_id)
  
  
  # Test the host we found for it's connection type
  if found_server['type'] == 'mysql_56':
    import mysql_handler
    handler = mysql_handler
  
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
  
  result = handler.CreateSchema(request, schema)
  
  return result


def ExtractSchema(request):
  """Export a schema, based on a spec, or everything"""
  handler = DetermineHandlerModule(request)
  
  result = handler.ExtractSchema(request)
  
  return result


def ExportSchema(request, path):
  """Export a schema, based on a spec, or everything"""
  handler = DetermineHandlerModule(request)
  
  handler.ExportSchema(request, path)
  
  return result


def UpdateSchema(request, schema):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.UpdateSchema(request, schema)
  
  return result


def ExportData(request, path):
  """Export/dump data from this datasource, based on spec, or everything"""
  handler = DetermineHandlerModule(request)
  
  result = handler.ExportData(request, path)
  
  return result


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
  
  result = handler.ImportData(request, path)
  
  return result


def GetInfoSchema(request):
  """Returns the record for this schema data (schema)"""
  handler = DetermineHandlerModule(request)
  
  result = handler.GetInfoSchema(request)
  
  return result


def GetInfoSchemaTable(request, schema, table_name):
  """Returns the record for this schema table data (schema_table)"""
  handler = DetermineHandlerModule(request)
  
  result = handler.GetInfoSchemaTable(request, schema, table_name)
  
  return result


def GetInfoSchemaAndTable(request, table_name):
  """Returns the record for this schema table data (schema_table)
  
  This is a helper function, calls GetInfoSchema() and GetInfoSchemaTable()
  """
  schema = GetInfoSchema(request)
  
  schema_table = GetInfoSchemaTable(request, schema, table_name)
  
  return (schema, schema_table)


def GetInfoSchemaTableField(request, schema_table, field_name):
  """Returns the record for this schema field data (schema_table_field)"""
  handler = DetermineHandlerModule(request)
  
  result = handler.GetInfoSchemaTableField(request, schema_table, field_name)
  
  return result


def GetUser(request, username=None):
  """Returns user.id (int)"""
  handler = DetermineHandlerModule(request)
  
  # We allow an explicit username, but otherwise use the requester's username
  if not username:
    username = request.username
  
  
  #TODO(g): This needs to be configurable, so we can specify where to get authentication information...
  user = handler.GetUser(request, username)
  
  return user


def RecordVersionsAvailable(request, table, record_id, username=None):
  """List all of the historical and currently available versions available for this record.
  
  Looks at 3 tables to figure this out: version_changelist_log (un-commited changes),
      version_commit_log (commited changes), version_working (single user changes)
  
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, primary key (ex: `id`) of the record in this table.  Use Filter() to use other field values
    username: string (default None), if not None, this is a specific user to check versions.  Otherwise the
        request.username is used.
    
  Returns: ...tbd...
  """
  handler = DetermineHandlerModule(request)
  
  # Get the user's ID
  user = GetUser(request, username)
  
  result = handler.RecordVersionsAvailable(request, table, record_id, user=user)
  
  return result


def CommitWorkingVersion(request):
  """Immediately takes everything in this user's Working Set and creates a change list.
  
  This is the short-cut for Change Management, so we don't need all the steps and still have versionining.
  
  See also: CreateChangeList() and CreateChangeListFromWorkingSet() and CommitWorkingVersionSingleRecord()
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.CommitWorkingVersion(request)
  
  return result


def CommitWorkingVersionSingleRecord(request, table, record_id):
  """TODO: Commit only a single record out of the working version.  Not all the working version records...
  
  Immediately takes a working version record and commits it, moving it through the rest of change management.
  
  This is the short-cut for Change Management, so we don't need all the steps and still have versionining.
  
  See also: CreateChangeList() and CreateChangeListFromWorkingSet() and CommitWorkingVersion()
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.CommitWorkingVersionSingleRecord(request, table, record_id)
  
  return result


def CreateChangeList(request, data):
  """Create a change list from the given table and record_id in the Working Set.
  
  This ensures we always have at least something in a change list, so we dont end up with empty ones where we dont know what
  went wrong.  We will always have some indication of what was going on if we find a change list.
  
  See also: CommitWorkingVersion() and CreateChangeListFromWorkingSet()
  
  Returns: int, version_number for this change list
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.CreateChangeList(request, record_id)
  
  return result


def CreateChangeListFromWorkingSet(request):
  """Create a Change List from the entire Working Set of version records.
  
  This is a fast way to prepare to commit everything that is currently being worked on.
  
  See also: CommitWorkingVersion() and CreateChangeList()
  
  Returns: int, version_number for this change list
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.CreateChangeListFromWorkingSet(request)
  
  return result


def AbandonWorkingVersion(request, table, record_id):
  """Abandon any current edits in version_working for this record.
  
  This does not effect Change Lists that are created, which must either be editted (removing record), or else
  the entire change list must be abandonded.
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.AbandonWorkingVersion(request, table, record_id)
  
  return result


def AbandonChangeList(request, change_list_id):
  """Abandon an open Change List, change_list_id==version_change.id
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.AbandonChangeList(request, change_list_id)
  
  return result


def Commit(request):
  """Commit a datasource transaction that is in the middle of a transaction."""
  handler = DetermineHandlerModule(request)
  
  result = handler.Commit(request)
  
  return result


def Set(request, table, data, version_management=False, commit_version=False, version_number=None, commit=True):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction, if not using version_managament.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record to set into table
    version_management: boolean (default False), if True this will use version management, if False will directly work with DB transactions
    commit_version: boolean (default False), if True, this will attempt to Commit the Version data after it has
        been stored in version_change as a single record update, without any additional VMCM actions
    version_number: int (default None), if an int, this is the version number in the version_change table to write to,
        if None this will create a new version_working entry
    commit: bool (default True), if not using version_management(==False) then this will be committed immediately,
        instead of waiting for a transactional Commit()
  
  Returns: int or None, if creating a new record this returns the newly created record primary key (ex: `id`), otherwise None
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.Set(request, table, data, version_management=version_management, commit_version=commit_version, version_number=version_number, commit=commit)
  
  return result


def SetVersion(request, table, data, commit_version=False, version_number=None):
  """Put (insert/update) data into this datasource's Version Management tables (working, unless version_number is specified).
  
  This is the same as Set() except version_managament=True, which is more explicit.  This should be easier to read and type,
  and it clearly does something different.  By default Set() will 
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record to set into table
    commit_version: boolean (default False), if True, this will attempt to Commit the Version data after it has
        been stored in version_change as a single record update, without any additional VMCM actions
    version_number: int (default None), if an int, this is the version number in the version_change table to write to,
        if None this will create a new version_working entry
  
  Returns: int or None, if creating a new record this returns the newly created record primary key (ex: `id`), otherwise None
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.SetVersion(request, table, data, commit_version=commit_version, version_number=version_number)
  
  return result


def SetFromUdnDict(request, data, version_number=None, use_working_version=True):
  """Set data from a dictionary with UDN keys (Universion Dotted Notation: 'table_name.row_id.field_name')
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    data: dict, record to set into table
    use_working_version: boolean (default True), if True and version_number==None this will also look at any
        version_working data and return it instead the head table data, if it exists for this user.
  """
  handler = DetermineHandlerModule(request)
  
  records = {}
  
  # Turn UDN into records
  for (udn, value) in data.items():
    (table, record_id, field) = udn.split('.')
    
    # Get the record from records, if it exists, otherwise create it
    record_key = (table, record_id)
    if record_key not in records:
      records[record_key] = {'id': record_id}
    
    record = records[record_key]
    
    record[field] = value
  
  # Set our data items
  for (record_key, record) in records.items():
    if not use_working_version:
      Set(request, table, record)
      
    else:
      SetVersion(request, table, record, version_number=version_number)
  


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
  
  result = handler.Get(request, table, record_id, version_number=version_number, use_working_version=use_working_version)
  
  return result


def Query(request, sql, params=None):
  """Perform a query without versioning.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    sql: string, SQL query to perform
    params: list of values, any value type will inserted into the query to be quoted properly
    
  Returns: dict, single record key/values
  """
  handler = DetermineHandlerModule(request)
  
  result = handler.Query(request, sql, params=params)
  
  return result


def Filter(request, table, data=None, order_list=None, groupby_list=None, version_number=None, use_working_version=True):
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  handler = DetermineHandlerModule(request)

  result = handler.Filter(request, table, data=data, order_list=order_list, groupby_list=groupby_list, version_number=version_number, use_working_version=use_working_version)
  
  return result


def Delete(request, table, record_id, version_number=None, use_working_version=True):
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  handler = DetermineHandlerModule(request)
  
  if version_number == None and use_working_version == None:
    result =  handler.Delete(request, table, record_id)
  else:
    raise Exception('TBD: Havent implemented Delete() for versions yet')
  
  
  return result


def DeleteFilter(request, table, data, version_number=None, use_working_version=True):
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  handler = DetermineHandlerModule(request)
  
  if version_number == None and use_working_version == None:
    result = handler.DeleteFilter(request, table, data)
    
  else:
    raise Exception('TBD: Havent implemented DeleteFilter() for versions yet')
  
  return result


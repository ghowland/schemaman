"""Data source handler.  Generic handler for all datasource types."""


import os
import threading
import time

from schemaman.utility.error import *
from schemaman.utility.path import *
import schemaman.utility as utility
from schemaman.utility.log import Log

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


def GetInfoSchemaAndTableById(request, table_id):
  """Returns the record for this schema table data (schema_table)
  
  This is a helper function, calls GetInfoSchema() and GetInfoSchemaTable()
  """
  handler = DetermineHandlerModule(request)
  
  (schema, schema_table) = handler.GetInfoSchemaAndTableById(request, table_id)
  
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


def AbandonCommit(request):
  """Abandon Commit a datasource transaction that is in the middle of a transaction."""
  handler = DetermineHandlerModule(request)
  
  result = handler.AbandonCommit(request)
  
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
  delete_records = []
  
  # Turn UDN into records
  for (udn, value) in data.items():
    # Check if we are deleting a record
    is_delete = False
    if udn.startswith('__delete.'):
      is_delete = True
      
      # Strip off the delete, as we know about it now
      (_, udn) = udn.split('.', 1)
      
    
    (table, record_id, field) = udn.split('.')
    record_id = int(record_id)
    
    # Get the record from records, if it exists, otherwise create it
    record_key = (table, record_id)
    
    # Get the record from positive records or negative (delete)
    if not is_delete:
      # If we dont have an entry for this record yet
      if record_key not in records:
        records[record_key] = {'id': record_id}
      record = records[record_key]
      
      record[field] = value
    
    # Else, we are deleting, so use those delete records (as a list of tuples, since we only need to track table/record_id)
    else:
      # We want to store negative records in here too, just to get rid of any positive-update fields that may exist, so we nullify them.
      if record_key not in delete_records:
        delete_records.append(record_key)


  # Remove anything from our update records, if it also has a delete record.  Delete records win at this level, and neutralize updates.
  #   The reason Deletes win, is that Deletes only show up when someone has made an explicit call to delete a record, but positive update
  #   values will show up every request, because they are always present.  This is why deletes nullify positive updates.
  for record_key in delete_records:
    print 'Testing delete key: %s' % str(record_key)
    # If we have this record in positive-update records, then delete it, because the Delete records nullify it
    if record_key in records:
      print 'Deleting key: %s' % str(record_key)
      del records[record_key]
  
  
  import pprint
  print '\n\nSet UDN Records:\n%s\n' % pprint.pformat(records)
  print '\nSet UDN Delete Records:\n%s\n\n' % pprint.pformat(delete_records)
  
  # Set our data items
  for (record_key, record) in records.items():
    # Get the table from the record key.  We dont need the record_id, that is just for uniqueness in the key.  Set*() takes the key from the 'id' field
    (table, _) = record_key
    
    if not use_working_version:
      Set(request, table, record)
      
    else:
      SetVersion(request, table, record, version_number=version_number)
  
  
  # Delete our specified data items
  for (table, record_id) in delete_records:
    if not use_working_version:
      Delete(request, table, record_id)
      
    else:
      DeleteVersion(request, table, record_id, version_number=version_number)


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


def Filter(request, table, data=None, use_working_version=False, order_list=None, groupby_list=None, version_number=None):
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, key is fields, value is the equality value.  Strict matching here.
    order_list: list of strings, fields to order by
    groupby_list: list of strings, fields to group by
    version_number: int (default None), if an int, this is the version number in the version_change or version_commit
        tables.  version_change is scanned before version_commit, as these are more likely to be requested.
    use_working_version: boolean (default True), if True and version_number==None this will also look at any
        version_working data and return it instead the head table data, if it exists for this user.
  """
  handler = DetermineHandlerModule(request)

  result = handler.Filter(request, table, data=data, order_list=order_list, groupby_list=groupby_list, version_number=version_number, use_working_version=use_working_version)
  
  return result


def GetWorkingVersionData(request, username=None):
  """Returns a dict or None, with the current working data (already parsed from `version_working.data_yaml`
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
  
  Returns: dict or None
  """
  handler = DetermineHandlerModule(request)

  result = handler.GetWorkingVersionData(request, username=username)
  
  return result


def AcquireLock(request, lock, timeout=None, sleep_interval=0.1):
  """Spin loops until we can get this lock.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    lock: string, lock name.  Will be unique
    timeout: float (optional), time in seconds before timeout
    sleep_interval: float, time to sleep between checking on the lock
  
  Returns: boolean, did we get the lock?  True = yes. False = no.  This only matters if timeout is set, otherwise we will wait forever
  """
  Log('Acquire Lock: %s' % lock)
  
  done = False
  started = time.time()
  
  while not done:
    duration = time.time() - started
    
    # try:
    if 1:
      Set(request, 'schema_lock', {'name':lock})
      
      # It worked, we are done
      done = True
    
    # #TODO(g): Get the correct exception type here, so we only catch insertion failures
    # except Exception, e:
    #   print 'AcquireLock: Failed: %s: %s: %s' % (lock, duration, e)
    #   
    #   if timeout and duration > timeout:
    #     return False
    #   
    #   # Sleep for the specified time
    #   time.sleep(sleep_interval)
  
  return True


def ReleaseLock(request, lock):
  """Releases a lock.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    lock: string, lock name.  Will be unique
    
  Returns: boolean, did we release the lock?  True = yes. False = no.  If false, the lock was not set (which can indicate a problem)
  """
  Log('Release Lock: %s' % lock)
  
  lock_list = Filter(request, 'schema_lock', {'name': lock})
  
  if not lock_list:
    return False
  
  lock_record = lock_list[0]
  
  Delete(request, 'schema_lock', lock_record['id'])
  
  return True


def GetSchemaTableRowLockKey(request, table, record_id, schema=None):
  """
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
  
  Returns: str, lock key for specified table and record_id
  """
  # Ensure we have these.  We allow them to be passed in as a performance optimization
  if not schema:
    (schema, schema_table) = GetInfoSchema(request)
  
  lock = '%s.%s.%s' % (schema['id'], table, record_id)
  
  return lock


def GetNextNegativeNumber(request, table):
  """Returns the next negative number for a given table, from `schema_table.next_negative_id`
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
  """
  (schema, schema_table) = GetInfoSchemaAndTable(request, table)
  
  # Perform an internal lock, so we cant race on this
  #TODO
  
  # Get the lock key for this schema table row
  lock = GetSchemaTableRowLockKey(request, table, schema_table['id'], schema=schema)

  try:
    # Get the lock, so we dont collide on this
    AcquireLock(request, lock)
    
    # Get the next negative from the current storage
    next_negative_id = table_row['next_negative_id']
    
    # Decrement the next negative ID, so we always get original ones, and they wont conflict
    table_row['next_negative_id'] -= 1
    
    # Save with the new decremented number
    Set(request, 'schema_table', table_row)
  
  finally:
    # Release the lock
    ReleaseLock(request, lock)
  
  return next_negative_id


def Delete(request, table, record_id):
  """Delete a single record.
  
  Use DeleteVersion() if you want version management over this delete.  This deleted the Real record.
  
  Use DeleteFilter() if you want to potentially delete many rows at once.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, primary key (ex: `id`) of the record in this table.  Use Filter() to use other field values
  """
  handler = DetermineHandlerModule(request)
  
  result =  handler.Delete(request, table, record_id)
    
  return result


def DeleteVersion(request, table, record_id, version_number=None):
  """Delete a single record from Working Version or a Pending Commit.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, primary key (ex: `id`) of the record in this table.  Use Filter() to use other field values
    version_number: int, this is the version number in the version_changelist.id
  
  Returns: None
  """
  if version_number != None:
    raise Exception('TBD: Version Number specific Deletes has not yet been implemented.')
  
  Log('Delete Version: %s: %s' % (table, record_id))
  
  (schema, schema_table) = GetInfoSchemaAndTable(request, table)
  
  user = GetUser(request)
  
  # Get the lock key for this schema table row
  lock = GetSchemaTableRowLockKey(request, table, record_id, schema=schema)
  
  try:
    # Acquire a lock, so we can work safely
    AcquireLock(request, lock)
    
    # Get the current working version
    version_working = Get(request, 'version_working', user['id'])
    
    # If we dont have a working version, make new dicts to store data in
    update_data = {}
    delete_data = {}
    
    # If, we have a working version, so get the data
    if version_working:
      update_data = utility.path.LoadYamlFromString(version_working['data_yaml'])
      delete_data = utility.path.LoadYamlFromString(version_working['delete_data_yaml'])
      
      if not update_data:
        update_data = {}
      
      if not delete_data:
        delete_data = {}

    
    # Check to see if this a Real record
    real_record = Get(request, table, record_id)
    
    # If we have a Real record, make an entry in the Delete Data, because we really want to delete this
    if real_record:
      
      # If we dont have the schema in our delete_data, add it
      if schema['id'] not in delete_data:
        delete_data[schema['id']] = {}
        
      # If we dont have the schema_table in our delete_data, add it
      if schema_table['id'] not in delete_data[schema['id']]:
        delete_data[schema['id']][schema_table['id']] = []
      
      # If we dont have this record in the proper place already (other records of that table to-delete), and this isnt a negative number, append it
      if record_id not in delete_data[schema['id']][schema_table['id']] and record_id >= 0:
        delete_data[schema['id']][schema_table['id']].append(record_id)
    
    # If we have an entry of this record in update_data, then remove that, because Delete always means to clear version data
    if schema['id'] in update_data:
      if schema_table['id'] in update_data[schema['id']]:
        if record_id in update_data[schema['id']][schema_table['id']]:
          # Delete the record from this update_data table, we are nulling that potential change
          del update_data[schema['id']][schema_table['id']][record_id]
    
    
    # Clean up the data, so we dont leave empty cruft around
    CleanEmptyVersionData(update_data)
    CleanEmptyVersionData(delete_data)
    
    # Add this to the working version record
    version_working['data_yaml'] = utility.path.DumpYamlAsString(update_data)
    version_working['delete_data_yaml'] = utility.path.DumpYamlAsString(delete_data)
    
    # Save the working version record
    Set(request, 'version_working', version_working)
  
  finally:
    # Ensure we release the lock
    ReleaseLock(request, lock)


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


def CleanEmptyVersionData(version_data):
  """Cleans up any empty dicts or lists in the version_work data_yaml field data."""
  schema_keys = version_data.keys()
  
  # Loop over our schemas
  for schema_key in schema_keys:
    schema_data = version_data[schema_key]
    table_keys = schema_data.keys()
    
    for table_key in table_keys:
      table_data = schema_data[table_key]
      
      # If this is a dict type, we need to go one more level deep.  If not, we dont
      if type(table_data) == dict:
        record_keys = table_data.keys()
        
        # Loop over our records
        for record_key in record_keys:
          # If the record is empty, delete it
          if not table_data[record_key]:
            del table_data[record_key]
      
      # If the table is empty, delete it
      if not schema_data[table_key]:
        del schema_data[table_key]
    
    # If this schema is empty, delete it
    if not version_data[schema_key]:
      del version_data[schema_key]


def ListOfDictsToValueList(list_of_dicts, key, convert=None):
    """Returns a list of values, from the key of a list of dicts.  May convert type of values.

    Args:
        list_of_dicts: list of dicts
        key: value (any, for key in the dicts)
        convert: type, examples: str, int, float, etc.  Will be used as a function to convert value types.  Can also pass in any function that takes 1 value and emits 1 value.

    Returns: list of values
    """
    result_list = []

    for item in list_of_dicts:
        value = item[key]

        if convert:
          value = convert(value)

        result_list.append(value)

    return result_list


def ListOfDictsToDict(list_of_dicts, key):
    """Returns a dict, from the key of a list of dicts.

    Args:
        list_of_dicts: list of dicts
        key: value (any, for key in the dicts)

    Returns: list of values
    """
    result = {}

    for item in list_of_dicts:
        result[item[key]] = item

    return result


def ListOfDictsToKeyValueDict(list_of_dicts, key, value_key):
    """Returns a dict, from the key of a list of dicts.

    Args:
        list_of_dicts: list of dicts
        key: value (any, for key in the dicts)

    Returns: list of values
    """
    result = {}

    for item in list_of_dicts:
        result[item[key]] = item[value_key]

    return result

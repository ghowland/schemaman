"""
Handle all SchemaMan datasource specific functions: MySQL
"""


import json

import datasource
from utility.log import Log

from query import *


# Debugging information logged?
SQL_DEBUG = True


def ReleaseConnections(request):
  """Release any connections tied with this request_number"""
  #TODO(g): Flatten this call path
  MySQLReleaseConnections(request)


def TestConnection(request):
  """Create a schema, based on a spec"""
  Log('MySQL: Test Connection: %s: %s' % (request.connection_data['alias'], request.request_number))
  
  connection = GetConnection(request)
  
  result = connection.Query('SHOW TABLES')
  
  return result


def CreateSchema(request):
  """Create a schema, based on a spec"""
  raise Exception('TBD...')
  pass


def ExtractSchema(request):
  """Export a schema, based on a spec, or everything"""
  Log('MySQL: Extract Schema: %s: %s' % (request.connection_data['alias'], request.request_number))
  
  connection = GetConnection(request, server_id=request.server_id)
  
  # Pack all our schema data in here
  data = {}
  
  tables = connection.Query("SHOW TABLES")
  
  for table in tables:
    key = table.keys()[0]
    table_name = table[key]
    
    data[table_name] = {}
    
    fields = connection.Query("DESC `%s`" % table_name)
    
    for item in fields:
      # schema_table_field: {u'Extra': u'auto_increment', u'Default': None, u'Field': u'id', u'Key': u'PRI', u'Null': u'NO', u'Type': u'int(11)'}
      print '%s: %s' % (table_name, item)
      
      data[table_name][item['Field']] = {}
      field = data[table_name][item['Field']]
      
      field['name'] = item['Field']
      field['default'] = item['Default']
      
      # If this is not a sized type
      if '(' not in item['Type']:
        field['type'] = item['Type']
        field['size1'] = None
        field['size2'] = None
      
      # Else, get the sizes for this type, as well as the type name
      else:
        (type_name, type_args) = item['Type'].split('(', 1)
        type_args = type_args.replace(')', '').split(',')
        
        field['type'] = type_name
        
        # Type Size
        if type_args:
          field['size1'] = int(type_args[0])
          
          if len(type_args) >= 2:
            field['size2'] = int(type_args[1])
          else:
            field['size2'] = None
      
      # Allow Null?
      if item['Null'] == 'YES':
        field['allow_null'] = True
      else:
        field['allow_null'] = False
      
      # Primary Key?
      if item['Key'] == 'PRI':
        field['pkey'] = True
      else:
        field['pkey'] = False
        
      # Multiple Primary Key?
      if item['Key'] == 'MUL':
        #TODO(g): Add multi-key support.
        print 'TODO(g): Add multi-key support.'
    
      # Get the unique constraints too.  These are what allow us to constrain our data for uniqueness, so it's important.  Separate from PKEY indexs.
  
  return data


def ExportSchema(request):
  """Export a schema, based on a spec, or everything"""
  raise Exception('TBD...')
  pass


def UpdateSchema(request):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  raise Exception('TBD...')
  pass


def ExportData(request):
  """Export/dump data from this datasource, based on spec, or everything"""
  raise Exception('TBD...')
  pass


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
  raise Exception('TBD...')
  pass


def GetUser(request, username):
  """Returns user.id (int)"""
  # Get a connection
  connection = GetConnection(request)
  
  #TODO(g): Need to specify the schema (DB) too, otherwise this is wrong...  Get from the request datasource info?  We populated, so we should know how it works...
  sql = "SELECT * FROM `user` WHERE name = %s"
  result = connection.Query(sql, [username])
  if not result:
    raise Exception('Unknown user: %s' % username)
  
  user = result[0]

  return user


def GetInfoSchema(request):
  """Returns the record for this schema data (schema)"""
  # Get a connection
  connection = GetConnection(request)
  
  # Get the schema name from our request.datasource.database
  database_name = request['datasource']['database']
  
  sql = "SELECT * FROM schema WHERE name = %s"
  result_schema = connection.Query(sql, [database_name])
  if not result_schema:
    raise Exception('Unknown schema: %s' % database_name)
  
  schema = result_schema[0]
  
  return schema


def GetInfoSchemaTable(request, schema, table):
  """Returns the record for this schema table data (schema_table)"""
  # Get a connection
  connection = GetConnection(request)
  
  # Get the schema name from our request.datasource.database
  database_name = request['datasource']['database']
  
  #TODO(g): Need to specify the schema (DB) too, otherwise this is wrong...  Get from the request datasource info?  We populated, so we should know how it works...
  sql = "SELECT * FROM schema_table WHERE schema_id = %s AND name = %s"
  result_schema_table = connection.Query(sql, [schema['id'], table])
  if not result_schema_table:
    raise Exception('Unknown schema_table: %s: %s' % (database_name, table))
  
  schema_table = result_schema_table[0]
  
  return schema_table


def GetInfoSchemaTableField(request, schema_table, name):
  """Returns the record for this schema field data (schema_table_field)"""
  # Get a connection
  connection = GetConnection(request)
  
  sql = "SELECT * FROM schema_table_field WHERE schema_table_id = %s AND name = %s"
  result_schema_table_field = connection.Query(sql, [schema['id'], table])
  if not result_schema_table_field:
    raise Exception('Unknown schema_table_field: %s: %s: %s' % (database_name, table, name))
  
  schema_table = result_schema_table_field[0]
  
  return schema_table


def RecordVersionsAvailable(request, table, record_id, user=user):
  """List all of the historical and currently available versions available for this record.
  
  Looks at 3 tables to figure this out: version_changelist_log (un-commited changes),
      version_commit_log (commited changes), version_working (single user changes)
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, primary key (ex: `id`) of the record in this table.  Use Filter() to use other field values
    username: string (default None), if not None, this is a specific user to check versions.  Otherwise the
        request.username is used.
    
  Returns: list of dicts, dicts have 'id' and 'name' fields.
  """
  # Get a connection
  connection = GetConnection(request)
  
  # Get the schema name from our request.datasource.database
  database_name = request['datasource']['database']
  
  # Get the schema
  schema = GetInfoSchema(request)
  
  # Get the schema table (pass in schema so we dont do it twice)
  schema_table = GetInfoSchemaTable(request, schema, table)
  
  
  # version_changelist_log
  sql = "SELECT * FROM `version_changelist_log` WHERE schema_id = %s AND schema_table_id = %s AND record_id = %s ORDER BY id"
  result_changelist = connection.Query(sql, [schema['id'], schema_table['id'], record_id])
  
  # version_commit_log
  sql = "SELECT * FROM `version_changelist_log` WHERE schema_id = %s AND schema_table_id = %s AND record_id = %s ORDER BY id"
  result_commit = connection.Query(sql, [schema['id'], schema_table['id'], record_id])
  
  # version_working
  sql = "SELECT * FROM `version_working` WHERE user_id = %s"
  result_working = connection.Query(sql, [user['id']])
  
  
  # Compile the final result list, from the found results
  result = []
  
  # Commited versions
  for item in result_commit:
    data = {'id': item['id'], 'name':'Commit Number: %s' % item['id']}
    result.append(data)
  
  # Change List versions
  for item in result_changelist:
    data = {'id': item['id'], 'name':'Pending Change Number: %s' % item['id']}
    result.append(data)
  
  # Working set
  if result_working:
    working = result_working[0]
    if schema_id in working['data_json']:
      if schema_table['id'] in working['data_json'][schema_id]:
        if record_id in working['data_json'][schema_id][schema_table['id']]:
          data = {'id':'working', 'name':'Working Version: %s' % user['name']}
          result.append(data)
  
  return result


def CommitWorkingVersion(request, table, record_id):
  """Immediately takes a working version record and commits it, moving it through the rest of change management.
  
  This is the short-cut for Change Management, so we don't need all the steps and still have versionining.
  
  See also: CreateChangeList() and CreateChangeListFromWorkingSet()
  """
  working_version = GetUserVersionWorkingRecord(request)
  
  record = GetRecordFromVersionRecord(request, working_version, table, record_id)
  
  # Make a single record entry in the version_changelist table, do all the work as we normally would (increments the PKEY, etc)
  version_number = CreateChangeList(request, table, record)
  
  # "commit" the changelist into version_commit, which will also put the data into the direct DB tables
  result = CommitChangeList(request, version_number)
  
  return result


def CommitChangeList(request, version_number):
  """Commit a pending change list (version_changelist) to the final data, and put in version_commit.
  
  Also updates version_changelist_log (removed entry), and version_commit_log (adds entry).
  
  This function works as a single datasbase transaction, so it cannot leave the data in an inconsistent state.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    version_number: int, this is the version number in the version_changelist.id
  
  Returns: None
  """
  # Get the specified change list record
  record = Get(request, 'version_changelist', version_number, use_working_version=False)
  
  # Create the new commit record to be inserted
  data = {'user_id':request.user['id'], 'data_json':record['data_json']}
  
  # Insert into version_commit
  version_commit_id = SetDirect(request, 'version_commit', data, commit=False)
  
  # Create the commit_log data
  CreateVersionLogRecords(request, 'version_commit_log', data, commit=False)
  
  # Create the version_commit_log row
  SetDirect(request, 'version_commit_log', commit_log_data, commit=False)
  
  # Remove the version_changelist_log row
  DeleteFilter(request, 'version_changelist_log', {'version_changelist_id':version_number}, commit=False)
  
  # Remove the version_changelist row
  Delete(request, 'version_changelist', record['id'], commit=False)
  
  # Commit the request
  Commit(request)


def CreateVersionLogRecords(request, table, version_id, data, commit=True):
  """Create all the rows needed in the `version_*_log` tables (specified by table) for the data
  
  Version table information to stored in data['data_json'] as JSON encoded dict, which is keyed
  on the schema.id (int), then a dict keyed on the schema_table.id (int), then the field names (string)
  for the final dict, with values of the table field values (varying types).
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    version_id: int, the PKEY of the table record we are creating log records for
    data: dict, record that we want to store in the table row
    commit: boolean (default True), if True any queries that could be commited will be (single query transaction), if False then a later Commit() will be required
  
  Returns: list of ints, all of the PKEYs for the log rows we inserted
  """
  # We will return a list of ints, which are all the row `id` field values (PKEYs) for the table records
  log_row_ids = []
  
  change = json.loads(data['data_json'])
  
  # Process all the schema table fields we need version logs for
  for (schema_id, schema_tables) in change.items():
    for (schema_table_id, records) in schema_tables.items():
      for record_id in records:
        # Create our reference field, based on the table name (ex: version_commit_id)
        reference_field = '%s_id' % table
        
        # Create the log record data to insert
        log_data = {reference_field: version_id, 'schema_id':schema_id, 'schema_table_id':schema_table_id, 'record_id':record_id}
        
        # Directly save this into the `version_*_log` table, with the commit flag specified
        SetDirect(request, table, log_data, commit=commit)
  
  return log_row_ids


def CreateChangeList(request, table, data):
  """Create a change list from the given table and record_id in the Working Set.
  
  This ensures we always have at least something in a change list, so we dont end up with empty ones where we dont know what
  went wrong.  We will always have some indication of what was going on if we find a change list.
  
  See also: CommitWorkingVersion() and CreateChangeListFromWorkingSet()
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record that we want to store in the table row
  
  Returns: int, version_number for this change list
  """
  raise Exception('TBD...')
  
  return version_number


def CreateChangeListFromWorkingSet(request):
  """Create a Change List from the entire Working Set of version records.
  
  This is a fast way to prepare to commit everything that is currently being worked on.
  
  See also: CommitWorkingVersion() and CreateChangeList()
  
  Returns: int, version_number for this change list
  """
  raise Exception('TBD...')
  
  return version_number


def AbandonWorkingVersion(request, table, record_id):
  """Abandon any current edits in version_working for this record.
  
  This does not effect Change Lists that are created, which must either be editted (removing record), or else
  the entire change list must be abandonded.

  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, record `id` field, primary key
  
  Returns: None
  """
  # Get this user's working version record
  record = GetUserVersionWorkingRecord(request)
  
  # Extract the data_json payload
  change = json.loads(record['data_json'])

  # Format record key
  #TODO(g): Do this properly with the above dynamic PKEY info.  Is this good enough because we take record_id?  Maybe this needs to already be turned into the data_key?  This definitely needs to be a First Class Citizen in schemaman
  data_key = record_id
  
  # Add this set data to the version change record, if it doesnt exist
  if schema['id'] not in change:
    raise Exception('This user does not have the specified record in their working version: %s: %s: %s' % (request.username, table, record_id))
  
  # Add this table to the change record, if it doesnt exist
  if schema_table['id'] not in change[schema['id']]:
    raise Exception('This user does not have the specified record in their working version: %s: %s: %s' % (request.username, table, record_id))
  
  # Delete the record from the working version data
  del change[schema['id']][schema_table['id']]
  
  # Put this change record back into the version_change table, so it's saved
  record['data_json'] = json.dumps(change)
  
  # Save the change record
  result_record = SetDirect(request, 'version_working', record)


def AbandonChangeList(request, change_list_id):
  """Abandon an open Change List, change_list_id==version_change.id
  """
  raise Exception('TBD...')
  
  return result


def GetUserVersionWorkingRecord(request, user_id=None):
  """Returns a dict (row) from the version_working table for this request.user"""
  # If we werent given an explicit user, use the request user
  if not user_id:
    user_id = request.user['id']
  
  # Get a connection
  connection = GetConnection(request)
  
  schema = datasource.GetInfoSchema(request)
  schema_table = datasou.GetInfoSchemaTable(request, schema, table)
  
  # Get the current working record for this user (if  any)
  sql = "SELECT * FROM version_working WHERE user_id = %s"
  result = connection.Query(sql, [user_id])
  
  # If we have a current change version record, use that
  if result:
    record = result[0]
  
  # Else, we dont have one yet, so create one
  else:
    raise Exception('No version working data exists for user: %s' % request.username)
  
  return record
  

def GetRecordFromVersionRecord(request, version_record, table, record_id):
  """Get a record from a version record (could be version_working, changelist or commit row record).
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, record `id` field, primary key
    
  Returns: dict, row record from the version record (stored in data_json field)
  """
  # Get the JSON payload from the version record
  change = json.loads(version_record['data_json'])
  
  # Format record key
  #TODO(g): Do this properly with the above dynamic PKEY info
  data_key = data['id']
  
  # Add this set data to the version change record, if it doesnt exist
  if schema['id'] not in change:
    raise RecordNotFound('Could not find version record: %s: %s: %s' % (request.username, table, record_id))
  
  # Add this table to the change record, if it doesnt exist
  if schema_table['id'] not in change[schema['id']]:
    raise RecordNotFound('Could not find version record: %s: %s: %s' % (request.username, table, record_id))
  
  # Ensure the record is in the table
  if data_key not in change[schema['id']][schema_table['id']]:
    raise RecordNotFound('Could not find version record: %s: %s: %s' % (request.username, table, record_id))
  
  return change[schema['id']][schema_table['id']][data_key]
  

def Commit(request):
  """Commit a datasource transaction that is in the middle of a transaction.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
  
  Returns: None
  """
  connection = GetConnection(request)
  
  connection.Commit()


def Set(request, table, data, version_management=True, commit_version=False, version_number=None, noop=False, update_returns_id=True, debug=SQL_DEBUG, commit=True):
  """Put (insert/update) data into this datasource.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record that we want to store in the table row
    commit_version: boolean (default False), if True, this will automatically commit this version as part of this set (in version_commit)
    version_number: int (default None), if an int, this is the version number in the version_change or version_commit
        tables.  version_change is scanned before version_commit, as these are more likely to be requested.
    noop: boolean (default False), if True do not actually query the database, (no operation)
    update_returns_id: boolean (default True), if True UPDATE will return the PKEY (ex: `id`) keeping the same results that INSERT does
    debug: boolean, if True will log more verbosely
    commit: boolean (default True), if True any queries that could be commited will be (single query transaction), if False then a later Commit() will be required

  Return: int or None, If commit_version==True then this is the real table's PKEY int, else None
  """
  if not version_management:
    SetDirect(request, table, data, version_management=version_management, commit_version=commit_version, version_number=version_number, noop=noop, update_returns_id=update_returns_id, debug=debug)
  
  else:
    SetVersion(request, table, data, version_management=version_management, commit_version=commit_version, version_number=version_number, noop=noop, update_returns_id=update_returns_id, debug=debug, commit=commit)


def SetVersion(request, table, data, version_management=True, commit_version=False, version_number=None, noop=False, update_returns_id=True, debug=SQL_DEBUG):
  """Put (insert/update) data into this datasource.  Writes into version management tables (working or changelist if version_number is specified)
  
  Works as a single transaction, as version data is always commited into the version_* tables.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record that we want to store in the table row
    commit_version: boolean (default False), if True, this will automatically commit this version as part of this set (in version_commit)
    version_number: int (default None), if an int, this is the version number in the version_change or version_commit
        tables.  version_change is scanned before version_commit, as these are more likely to be requested.
    update_returns_id: boolean (default True), if True UPDATE will return the PKEY (ex: `id`) keeping the same results that INSERT does
    debug: boolean, if True will log more verbosely

  Return: int or None, If commit_version==True then this is the real table's PKEY int, else None
  """
  # Get a connection
  connection = GetConnection(request)
  
  schema = datasource.GetInfoSchema(request)
  schema_table = datasou.GetInfoSchemaTable(request, schema, table)
  
  # If this is a working version (no version number)
  if not version_number:
    #TODO(g): Use functions like GetUserVersionWorkingRecord()?  Maybe not, I dont think it will be helpful.  Wait until later to determine how to unify this better, once I've written all the functions...  Still early...
    # Get the current working record for this user (if  any)
    sql = "SELECT * FROM version_working WHERE user_id = %s"
    result = connection.Query(sql, [request.user['id']])

  
  # Else, there is a version_number, so work with the version_changelist record
  else:
    # Get the current working record for this user (if  any)
    #NOTE(g):SECURITY: Im not forcing that only the owner can write these here.  Is that wrong?  Should I?  I think there should be a different authorization phase...
    sql = "SELECT * FROM version_changelist WHERE id = %s"
    result = connection.Query(sql, [version_number)
    
    # Fail if we cant find this record
    if not result:
      raise Exception('The pending changelist was not found: %s' % version_number)
  
  
  # Get the record data set up
  if result:
    record = result[0]
    change = json.loads(record['data_json'])
  
  # Else, we dont have one yet, so create one (this can only execute on working, because we fail if we have not result with pending)
  else:
    record = {'user_id':request.user['id'], 'data_json':{}}
    change = {}
  
  
  # Get the primary key information for this table, and determine how to format our key
  pass

  # Format record key
  #TODO(g): Do this properly with the above dynamic PKEY info
  data_key = data['id']
  
  # Add this set data to the version change record, if it doesnt exist
  if schema['id'] not in change:
    change[schema['id']] = {}
  
  # Add this table to the change record, if it doesnt exist
  if schema_table['id'] not in change[schema['id']]:
    change[schema['id']][schema_table['id']] = {}
  
  # Readability variable
  change_table = change[schema['id']][schema_table['id']]
  
  # Add this specific record
  change_table[data_key] = data
  
  # Put this change record back into the version_change table, so it's saved
  record['data_json'] = json.dumps(change_table)
  
  # Save the change record
  result_record = SetDirect(request, 'version_change', record)
  
  return result_record


def SetDirect(request, table, data, noop=False, update_returns_id=True, debug=SQL_DEBUG, commit=True):
  """Put (insert/update) data into this datasource.  Directly writes to database.
  
  Works as a single transaction if commit==True.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record that we want to store in the table row
    noop: boolean (default False), if True do not actually query the database, (no operation)
    update_returns_id: boolean (default True), if True UPDATE will return the PKEY (ex: `id`) keeping the same results that INSERT does
    debug: boolean, if True will log more verbosely
    commit: boolean (default True), if True any queries that could be commited will be (single query transaction), if False then a later Commit() will be required
  """
  # INSERT values into a table, and if they already exist, perform an UPDATE on the fields
  base_sql = "INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s"
  
  # Wrap all keys in backticks, so they cannot conflict with SQL keywords
  keys = data.keys()
  keys.sort()
  keys_ticked = []
  update_sets = []
  values = []
  value_format_list = []
  
  
  # Get our backticked wrapped insert keys, our value list, and our update setting
  for count in range(0, len(keys)):
    # Back tick column names
    ticked_key = '`%s`' % keys[count]
    keys_ticked.append(ticked_key)
    
    # Value list
    values.append(data[keys[count]])
    value_format_list.append('%s')
    
    # Update keys will reference the insert keys, so we dont have to specify the data twice (SQL does it)
    #TODO(g): Should I remove the primary key from this?  Not sure it's necessary.  Remove comment when proven it works without removing it (simpler)...
    update_sets.append('%s=VALUES(%s)' % (ticked_key, ticked_key))
  
  
  # Build out strings to insert into our base_sql
  insert_columns = ', '.join(keys_ticked)
  value_format_str = ', '.join(value_format_list)
  update_sql = ', '.join(update_sets)
  
  
  # Create our final SQL
  sql = base_sql % (table, insert_columns, value_format_str, update_sql)
  
  # Get a connection
  connection = GetConnection(request)
  
  
  # Query.  Will return a row_id (int) for INSERT, and None for Update
  if not noop:
    result = connection.Query(sql, values, commit=commit)
    
    # If we did an Update, we really want the 'id' field returned, like INSERT does (consistency and not having to do this all the time after an update)
    if result == 0 and update_returns_id:
      # This should always return a single dict in a list, due to our uniqueness constraints
      rows = Filter(connection_data, table, data, request_number)
      result = rows[0]['id']
    
  else:
    Log('Query not run, noop = True')
    result = None
  
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
  # Get a connection
  connection = GetConnection(request)
  
  #TODO(g): Confirm this is the primary key name, not just "id" all the time.  Can look this up in our schema_data_paths from connection_data...
  #TODO(g): Allow multiple fields for primary key, and do the right thing with them
  sql = "SELECT * FROM `%s` WHERE id = %s" % (table, int(record_id))
  result = connection.Query(sql)
  
  if result:
    record = result[0]
  else:
    record = None
  
  return record


def Filter(request, table, data):
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """  
  base_sql = "SELECT * FROM `%s` WHERE %s"
  
  keys = data.keys()
  keys.sort()
  
  keys_ticked = []
  where_list = []
  values = []
  
  
  # Get our backticked wrapped insert keys, our value list, and our update setting
  for count in range(0, len(keys)):
    # Skip any fields that are NULL.  They are not helping us here, and cause problems with "=" vs "IS", because SQL implements NULL testing stupidly
    if data[keys[count]] == None:
      continue
    
    # Back tick column names
    ticked_key = '`%s`' % keys[count]
    keys_ticked.append(ticked_key)
    
    # Update keys will reference the insert keys, so we dont have to specify the data twice (SQL does it)
    #TODO(g): Should I remove the primary key from this?  Not sure it's necessary.  Remove comment when proven it works without removing it (simpler)...
    where_list.append('%s = %%s' % ticked_key)
    
    # Values are passed in separate than the SQL string
    values.append(data[keys[count]])
  
  
  # Build out strings to insert into our base_sql
  where_sql = ' AND '.join(where_list)
  
  # Create our final SQL
  sql = base_sql % (table, where_sql)
  
  # Log('\n\nGetFromData: %s: %s\nSQL:%s\nValues:%s\n' % (table, data, sql, values))
  
  
  # Get a connection
  connection = GetConnection(request)
  
  # Query
  rows = connection.Query(sql, values)
  
  return rows


def Delete(request, table, record_id, noop=False, commit=True):
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()

  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, primary key (ex: `id`) of the record in this table.  Use Filter() to use other field values
    noop: boolean (default False), if True do not actually query the database, (no operation)
    commit: boolean (default True), if True any queries that could be commited will be (single query transaction), if False then a later Commit() will be required
  
  Returns: None
  """
  # Get a connection
  connection = GetConnection(request)
  
  # Construct the query
  sql = "DELETE FROM `%s` WHERE id = %%s" % table
  
  # Delete the record
  if not noop:
    connection.Query(sql, [record_id], commit=commit)
  else:
    Log('Delete NO-OP: %s: %s' % (table, record_id))


def DeleteFilter(request, table, data, noop=False, commit=True):
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, keys are strings (field names) and values are the values that must match (WHERE field=value)
    noop: boolean (default False), if True do not actually query the database, (no operation)
    commit: boolean (default True), if True any queries that could be commited will be (single query transaction), if False then a later Commit() will be required
  
  Returns: None
  """
  # Get a connection
  connection = GetConnection(request)
  
  # INSERT values into a table, and if they already exist, perform an UPDATE on the fields
  base_sql = "DELETE FROM `%s` WHERE %%s" % table
  
  # Wrap all keys in backticks, so they cannot conflict with SQL keywords
  keys = data.keys()
  keys.sort()
  values = []
  where_format_list = []
  
  # Get our backticked wrapped insert keys, our value list, and our update setting
  for count in range(0, len(keys)):
    # Add the value (passed in as params list)
    values.append(data[keys[count]])
    
    # Back tick column names
    ticked_key = '`%s`' % keys[count]
    
    # Add the field's where clause, value will be added through param list
    where_format_list.append('%s = %%s' % ticked_key)
  
  # Generate where format string from our list, always AND for this filter.  More complex logic can be done directly through SQL, it's better than trying to wrap all options
  where_format = ' AND '.join(where_format_list)
  
  # Create our final SQL
  sql = base_sql % where_format
  
  # If we want to perform this operation (not no-op)
  if not noop:
    connection.Query(sql, values, commit=commit)
    
  else:
    Log('Delete Filter NO-OP: %s: %s' % (sql, values))


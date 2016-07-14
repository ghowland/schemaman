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
  
  print 'MySQL: Test Connection: %s: %s' % (request.connection_data['alias'], request.request_number)
  
  connection = GetConnection(request)
  
  result = connection.Query('SHOW TABLES')
  
  return result


def CreateSchema(request):
  """Create a schema, based on a spec"""
  pass


def ExtractSchema(request):
  """Export a schema, based on a spec, or everything"""
  print 'MySQL: Extract Schema: %s: %s' % (request.connection_data['alias'], request.request_number)
  
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
  pass


def UpdateSchema(request):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  pass


def ExportData(request):
  """Export/dump data from this datasource, based on spec, or everything"""
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


def Commit(request):
  """Commit a datasource transaction that is in the middle of a transaction."""
  connection = GetConnection(request)
  
  result = connection.Commit()
  
  return result


def Set(request, table, data, version_management=True, commit_version=False, version_number=None, noop=False, update_returns_id=True, debug=SQL_DEBUG, commit=True):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  """
  if not version_management:
    SetDirect(request, table, data, version_management=version_management, commit_version=commit_version, version_number=version_number, noop=noop, update_returns_id=update_returns_id, debug=debug)
  
  else:
    SetVersion(request, table, data, version_management=version_management, commit_version=commit_version, version_number=version_number, noop=noop, update_returns_id=update_returns_id, debug=debug, commit=commit)


def SetVersion(request, table, data, version_management=True, commit_version=False, version_number=None, noop=False, update_returns_id=True, debug=SQL_DEBUG):
  """Put (insert/update) data into this datasource.  Writes into version management tables (working or changelist if version_number is specified)
  
  Works as a single transaction, as version data is always commited into the version_* tables.
  """
  # Get a connection
  connection = GetConnection(request)
  
  schema = datasource.GetInfoSchema(request)
  schema_table = datasou.GetInfoSchemaTable(request, schema, table)
  
  # If this is a working version (no version number)
  if not version_number:
    # Get the current working record for this user (if  any)
    sql = "SELECT * FROM version_working WHERE user_id = %s"
    result = connection.Query(sql, [request.user['id']])
    
    # If we have a current change version record, use that
    if result:
      record = result[0]
      change = json.loads(record['data_json'])
    
    # Else, we dont have one yet, so create one
    else:
      record = {'user_id':request.user['id'], 'data_json':{}}
      change = {}

  
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


def SetDirect(request, table, data, version_management=True, commit_version=False, version_number=None, noop=False, update_returns_id=True, debug=SQL_DEBUG, commit=True):
  """Put (insert/update) data into this datasource.  Directly writes to database.
  
  Works as a single transaction if commit==True.
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


def Delete(request):
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  pass


def DeleteFilter(request):
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  pass


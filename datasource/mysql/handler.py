"""
Handle all SchemaMan datasource specific functions: MySQL
"""

import datasource

from utility.log import Log

from query import *


# Debugging information logged?
SQL_DEBUG = True


def TestConnection(connection_data, request_number):
  """Create a schema, based on a spec"""
  
  print 'MySQL: Test Connection: %s: %s' % (connection_data['alias'], request_number)
  
  connection = GetConnection(connection_data, request_number, server_id=None)
  
  result = connection.Query('SHOW TABLES')
  
  return result


def CreateSchema(connection_data, request_number):
  """Create a schema, based on a spec"""
  pass


def ExtractSchema(connection_data, request_number):
  """Export a schema, based on a spec, or everything"""
  print 'MySQL: Extract Schema: %s: %s' % (connection_data['alias'], request_number)
  
  connection = GetConnection(connection_data, request_number, server_id=None)
  
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


def ExportSchema(connection_data, request_number):
  """Export a schema, based on a spec, or everything"""
  pass


def UpdateSchema(connection_data, request_number):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  pass


def ExportData(connection_data, request_number):
  """Export/dump data from this datasource, based on spec, or everything"""
  pass


def ImportData(connection_data, request_number, drop_first=False, transaction=False):
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


def Set(connection_data, table, data, request_number, noop=False, update_returns_id=True, debug=SQL_DEBUG):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
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
  connection = GetConnection(connection_data, request_number, server_id=None)
  
  
  # Query.  Will return a row_id (int) for INSERT, and None for Update
  if not noop:
    result = connection.Query(sql, values)
    
    # If we did an Update, we really want the 'id' field returned, like INSERT does (consistency and not having to do this all the time after an update)
    if result == 0 and update_returns_id:
      # This should always return a single dict in a list, due to our uniqueness constraints
      rows = Filter(connection_data, table, data, request_number)
      result = rows[0]['id']
    
  else:
    Log('Query not run, noop = True')
    result = None
  
  return result


def Get(connection_data, table, record_id, request_number):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    connection_data: dict, Connection Specificatin for Data Set
    table: string, ...
    data: dict, ...
    request_number: int, Transactional request number
  
  Returns: dict, single record key/values
  """
  # Get a connection
  connection = GetConnection(connection_data, request_number, server_id=None)
  
  #TODO(g): Confirm this is the primary key name, not just "id" all the time.  Can look this up in our schema_data_paths from connection_data...
  #TODO(g): Allow multiple fields for primary key, and do the right thing with them
  sql = "SELECT * FROM `%s` WHERE id = %s" % (table, int(record_id))
  result = connection.Query(sql)
  
  if result:
    record = result[0]
  else:
    record = None
  
  return record


def Filter(connection_data, table, data, request_number):
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
  connection = GetConnection(connection_data, request_number, server_id=None)
  
  # Query
  rows = connection.Query(sql, values)
  
  return rows


def Delete(connection_data, request_number):
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  pass


def DeleteFilter(connection_data, request_number):
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  pass



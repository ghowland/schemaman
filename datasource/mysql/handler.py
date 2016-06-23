"""
Handle all SchemaMan datasource specific functions: MySQL
"""


from query import *


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


def Put(connection_data, request_number, record):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  """
  pass


def Get(connection_data, table, data, request_number):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    connection_data: dict, Connection Specificatin for Data Set
    table: string, ...
    data: dict, ...
    request_number: int, Transactional request number
  
  Returns: dict, single record key/values
  """
  keys = list(data.keys())
  if len(keys) != 1:
    raise Exception('There should be exactly 1 key at the top level of the data dictionary, which is the table name.')
  
  table = keys[0]
  
  # If we have an 'id' field
  #TODO(g): Confirm this is the primary key name, not just "id" all the time
  #TODO(g): Allow multiple fields for primary key, and do the right thing with them
  if 'id' in data:
    sql = "SELECT * FROM %s WHERE id = %s" % (table, data['id'])
    result = Query(sql)
    
    return result
    
  else:
    raise Exception('No "id" field in data, other methods of selection not yet implemented...')


def Filter(connection_data, table, data, request_number):
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  
  print 'TODO(g): Filter database connections: %s: %s' % (table, data)
  
  return []


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



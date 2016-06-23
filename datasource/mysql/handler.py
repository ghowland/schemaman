"""
Handle all SchemaMan datasource specific functions: MySQL
"""


from query import *


def CreateSchema():
  """Create a schema, based on a spec"""
  pass


def ExtractSchema():
  """Export a schema, based on a spec, or everything"""
  pass


def ExportSchema():
  """Export a schema, based on a spec, or everything"""
  pass


def UpdateSchema():
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  pass


def ExportData():
  """Export/dump data from this datasource, based on spec, or everything"""
  pass


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
  pass


def Put():
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  """
  pass


def Get(schema, source, data):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    schema: dict, ...
    source: string, ...
    data: dict, ...
  
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


def Filter():
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  pass


def Delete():
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  pass


def DeleteFilter():
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  pass



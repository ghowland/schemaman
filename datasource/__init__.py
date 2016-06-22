"""
Datasource Module
"""


# Schema Datasource Utility functions
import utility


# Data Source Type handlers
import mysql


def DetermineHandlerModule(dataset):
  """Returns the handler module, which can handle these requests."""
  
  if dataset['type'] == 'mysql_56':
    return mysql
  
  
  raise Exception('Unknown Data Source Type: %s' % dataset['type'])


def CreateSchema(dataset, schema):
  """Create a schema, based on a spec"""
  handler = DetermineHandlerModule(dataset)
  
  handler.CreateSchema(dataset, schema)


def ExportSchema(dataset, path):
  """Export a schema, based on a spec, or everything"""
  handler = DetermineHandlerModule(dataset)
  
  handler.ExportSchema(dataset, path)


def UpdateSchema(dataset, schema):
  """Update a schema, based on a spec.
  
  Can go 'forward' or 'backwards' for version control, its still updating.
  """
  handler = DetermineHandlerModule(dataset)
  
  handler.UpdateSchema(dataset, schema)


def ExportData(dataset, path):
  """Export/dump data from this datasource, based on spec, or everything"""
  handler = DetermineHandlerModule(dataset)
  
  handler.ExportData(dataset, path)


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
  handler = DetermineHandlerModule(dataset)
  
  handler.ImportData(dataset, path)


def Put(dataset, data):
  """Put (insert/update) data into this datasource.
  
  Works as a single transaction.
  """
  handler = DetermineHandlerModule(dataset)
  
  handler.Put(dataset, data)


def Get(schema, source, data):
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  
  Args:
    schema: dict, ...
    source: string, ...
    data: dict, ...
  
  Returns: dict, single record key/values
  """
  handler = DetermineHandlerModule(dataset)
  
  handler.Get(dataset, data)


def Filter():
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  handler = DetermineHandlerModule(dataset)
  
  handler.Filter(dataset, data)


def Delete():
  """Delete a single record.
  
  NOTE(g): Processes single record deletes directly, sends fitlered deletes to DeleteFilter()
  """
  handler = DetermineHandlerModule(dataset)
  
  handler.Delete(dataset, data)


def DeleteFilter():
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  
  Works as a single transaction.
  
  NOTE(g): This is called by Delete(), and is not invoked from the CLI directly.
  """
  handler = DetermineHandlerModule(dataset)
  
  handler.DeleteFilter(dataset, data)


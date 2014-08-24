"""
Handle all SchemaMan datasource specific functions: MySQL
"""


from query import *


def CreateSchema():
  """Create a schema, based on a spec"""
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


def ImportData():
  """Import/load data to this datasource, based on spec, or everything"""
  pass


def Put():
  """Put (insert/update) data into this datasource."""
  pass


def Get():
  """Get (select single record) from this datasource.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  pass


def Filter():
  """Get 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables'.
  """
  pass


def Delete():
  """Delete a single record."""
  pass


def DeleteFilter():
  """Delete 0 or more records from the datasource, based on filtering rules.
  
  Can be a 'view', combining several lower level 'tables', which makes it a
  cascading delete.
  """
  pass



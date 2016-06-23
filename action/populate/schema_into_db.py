"""
Actions: Populate: Schema Into DB

Import a schema into a set of database tables:  database, table, field
"""



# SchemaMan libraries
import utility
from utility.interactive_input import *
import datasource


def Action(connection_data, action_input_args):
  """Perform action: Populate schema into DB"""
  print 'Populate Schema Into DB'
  
  data = {}
  
  # Get the Database table name
  intro_string = 'Name of table to put the Database in.'
  GetInputField(data, 'table_database', 'Database Table', 'database table', intro_string, force_strip=True)
  
  # Get the Table table name
  intro_string = 'Name of table to put the Tables in.'
  GetInputField(data, 'table_table', 'Table Table', 'table table', intro_string, force_strip=True)
  
  # Get the Field table name
  intro_string = 'Name of table to put the Fields in.'
  GetInputField(data, 'table_field', 'Field Table', 'field table', intro_string, force_strip=True)

  
  print '\nThese are the tables you specified:'
  print '  - Database: %s' % data['table_database']
  print '  - Table: %s' % data['table_table']
  print '  - Field: %s' % data['table_field']
  print
  print 'Is this correct?  (Enter Yes or this action will abort)'
  
  confirmation = ReadLine('$ ')
  
  # Aborting
  if confirmation.lower() != 'yes':
    return '\nAborting...'
  
  
  # Load all the schemas, and overlay them to get the sum of all of them (in order, so deterministic, last write wins)
  total_schema = {}
  for schema_path in connection_data['schema_paths']:
    schema = utility.path.LoadYaml(schema_path)
    
    total_schema.update(schema)
  
  
  # Put these tables for this database into the target schema
  target_connection_data = action_input_args[0]
  
  
  
  
  
  


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
  target_connection_data = datasource.LoadConnectionSpec(action_input_args[0])
  
  
  database_name = connection_data['datasource']['database']
  
  print 'Populating Schema for Database: %s' % database_name
  
  
  # Get our request number.  We can use this with all data sources.
  request_number = datasource.GetRequestNumber()
  
  
  # Get the existing entry for this, if it exists
  schema_database_list = datasource.Filter(target_connection_data, data['table_database'], {'name':data['table_database']}, request_number=request_number)
  
  print 'Filter: Schema DB: %s' % schema_database_list
  
  # If we dont already have this record
  if not schema_database_list:
    # Create it, and get it's record again so we have it's ID
    schema_record = {'name':data['table_database'], 'mysql_user':connection_data['datasource']['user'],
                     'mysql_password_path':connection_data['datasource']['password_path'],
                     'mysql_hostname':connection_data['datasource']['servers'][0]['host']}
    schema_database_id = datasource.Set(target_connection_data, data['table_database'], schema_record, request_number=request_number)
    
    print 'Created record: %s' % schema_database_id
    
    # Get the record we just put in
    schema_database = datasource.Get(target_connection_data, data['table_database'], schema_database_id, request_number=request_number)

    print 'Fetched record: %s' % schema_database
  
  else:
    schema_database = schema_database_list[0]
  
  
  # Loop over all our tables
  for (table, table_data) in schema.items():
    print 'Populating Schema for Table: %s' % table

    table_record = {'name':table, 'schema_id':schema_database['id']}
    schema_table_list = datasource.Filter(target_connection_data, data['table_table'], table_record, request_number=request_number)
    
    # If we dont have this table, create it
    if not schema_table_list:
      schema_table_id = datasource.Set(target_connection_data, data['table_table'], table_record, request_number=request_number)
    
    else:
      schema_table_id = schema_table_list[0]['id']
    
    for (field, field_data) in table_data.items():
      print 'Populating Schema for Field: %s: %s' % (table, field)
    
      field_record = {'name':field, 'schema_table_id':schema_table_id}
      schema_field_list = datasource.Filter(target_connection_data, data['table_field'], field_record, request_number=request_number)
      
      # If we dont have this table, create it
      if not schema_field_list:
        # Determine the Value Type ID
        #TODO(g): This isnt always part of the process...  We need some other way to do this update, and a post-script or something...?
        if field_data['type'] == 'int':
          value_type_id = 2
        elif field_data['type'] == 'varchar':
          value_type_id = 1
        elif field_data['type'] == 'text':
          value_type_id = 1
        else:
          raise Exception('Unknown value type: %s' % field_data['type'])
        
        if field.endswith('data_json'):
          value_type_id = 11
        
        # We want to set all the data, we only wanted to search and table/fieldname
        field_record = {'name':field, 'schema_table_id':schema_table_id, 'is_primary_key':field_data['pkey'],
                        'allow_null':field_data['allow_null'], 'default_value':field_data['default'], 'value_type_id':value_type_id}
        schema_field_id = datasource.Set(target_connection_data, data['table_field'], field_record, request_number=request_number)
    
    
  
  
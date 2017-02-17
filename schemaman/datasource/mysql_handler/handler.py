"""
Handle all SchemaMan datasource specific functions: MySQL
"""

import pprint

import schemaman.datasource as datasource
import schemaman.utility as utility
from schemaman.utility.log import Log
import schemaman.utility.data_control as data_control

import schemaman.datasource.cache as cache

from query import *


# Debugging information logged?
SQL_DEBUG = True


class InvalidArguments(Exception):
  """Something wasnt right with the args."""


class RecordNotFound(Exception):
  """Couldnt find the record."""


def ReleaseConnections(request):
  """Release any connections tied with this request_number"""
  #TODO(g): Flatten this call path
  MySQLReleaseConnections(request)


def TestConnection(request):
  """Create a schema, based on a spec"""
  Log('MySQL: Test Connection: %s: %s' % (request.connection_data['alias'], request.request_number))
  
  connection = GetConnection(request)
  
  #TODO(g): Make a better connection test?  SELECT 1?
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


def GetUser(request, username=None, use_cache=True):
  """Returns user.id (int)"""
  # We allow an explicit username, but otherwise use the requester's username
  if not username:
    username = request.username
  
  # If we want to use the cache, and we have this is cache, return it
  if use_cache:
    user = cache.Get('user_by_name', username)
    if user != cache.NoCacheResultFound:
      return user
  
  # Get a connection
  connection = GetConnection(request)
  
  #TODO(g): Need to specify the schema (DB) too, otherwise this is wrong...  Get from the request datasource info?  We populated, so we should know how it works...
  sql = "SELECT * FROM `user` WHERE name = %s"
  result = connection.Query(sql, [username])
  if not result:
    raise Exception('Unknown user: %s' % username)
  
  user = result[0]

  # Save this is cache
  cache.Set('user_by_name', username, user)
  cache.Set('user_by_id', user['id'], user)

  return user


def GetUserById(request, user_id, use_cache=True):
  """Returns user record (dict)"""
  # Get a connection
  connection = GetConnection(request)
  
  # If we want to use the cache, and we have this is cache, return it
  if use_cache:
    user = cache.Get('user_by_id', user_id)
    if user != cache.NoCacheResultFound:
      return user
  
  #TODO(g): Need to specify the schema (DB) too, otherwise this is wrong...  Get from the request datasource info?  We populated, so we should know how it works...
  sql = "SELECT * FROM `user` WHERE id = %s"
  result = connection.Query(sql, [user_id])
  if not result:
    raise Exception('Unknown user: %s' % user_id)
  
  user = result[0]

  # Save this is cache
  cache.Set('user_by_name', user['name'], user)
  cache.Set('user_by_id', user['id'], user)
  
  return user


def GetInfoSchema(request):
  """Returns the record for this schema data (schema)"""
  # Get this cached result, and return it if available
  cache_result = cache.Get('schema', request.connection_data['datasource']['database'])
  if cache_result != cache.NoCacheResultFound:
    return cache_result

  # Get a connection
  connection = GetConnection(request)
  
  # Get the schema name from our request.datasource.database
  database_name = request.connection_data['datasource']['database']
  
  sql = "SELECT * FROM `schema` WHERE `name` = %s"
  result_schema = connection.Query(sql, [database_name])
  if not result_schema:
    raise Exception('Unknown schema: %s' % database_name)
  
  schema = result_schema[0]
  
  # Set this cache result
  cache.Set('schema', request.connection_data['datasource']['database'], schema)
  
  return schema


def GetInfoSchemaTable(request, schema, table, filter_key='name'):
  """Returns the record for this schema table data (schema_table)"""
  # Get this cached result, and return it if available
  cache_result = cache.Get('schema_table__%s' % filter_key, (schema['id'], table))
  if cache_result != cache.NoCacheResultFound:
    return cache_result

  # Get a connection
  connection = GetConnection(request)
  
  #TODO(g): Need to specify the schema (DB) too, otherwise this is wrong...  Get from the request datasource info?  We populated, so we should know how it works...
  sql = "SELECT * FROM `schema_table` WHERE schema_id = %%s AND %s = %%s" % filter_key
  result_schema_table = connection.Query(sql, [schema['id'], table])
  if not result_schema_table:
    raise Exception('Unknown schema_table: %s: %s' % (request.connection_data['datasource']['database'], table))
  
  schema_table = result_schema_table[0]
  
  # Save the cache result
  cache.Set('schema_table__%s' % filter_key, (schema['id'], table), schema_table)
  
  return schema_table


def GetInfoSchemaAndTable(request, table_name):
  """Returns the record for this schema table data (schema_table)
  
  This is a helper function, calls GetInfoSchema() and GetInfoSchemaTable()
  """
  # Get this cached result, and return it if available
  cache_result = cache.Get('schema_and_table', (request.connection_data['datasource']['database'], table_name))
  if cache_result != cache.NoCacheResultFound:
    return cache_result

  schema = GetInfoSchema(request)
  
  schema_table = GetInfoSchemaTable(request, schema, table_name)
  
  # Save the cache result
  cache.Get('schema_and_table', (request.connection_data['datasource']['database'], table_name), (schema, schema_table))
  
  return (schema, schema_table)


def GetInfoSchemaAndTableById(request, schema_table_id):
  """Returns the record for this schema table data (schema_table)
  
  This is a helper function, calls GetInfoSchema() and GetInfoSchemaTable()
  """
  schema = GetInfoSchema(request)
  
  schema_table = GetInfoSchemaTable(request, schema, schema_table_id, filter_key='id')
  
  return (schema, schema_table)


def GetInfoSchemaTableField(request, schema_table, name):
  """Returns the record for this schema field data (schema_table_field)"""
  # Get a connection
  connection = GetConnection(request)
  
  sql = "SELECT * FROM `schema_table_field` WHERE schema_table_id = %s AND name = %s"
  result_schema_table_field = connection.Query(sql, [schema['id'], table])
  if not result_schema_table_field:
    raise Exception('Unknown schema_table_field: %s: %s: %s' % (request.connection_data['datasource']['database'], table, name))
  
  schema_table = result_schema_table_field[0]
  
  return schema_table


def RecordVersionsAvailable(request, table, record_id, user=None):
  """List all of the historical and currently available versions available for this record.
  
  Looks at 3 tables to figure this out: version_pending_log (un-commited changes),
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
  database_name = request.connection_data['datasource']['database']
  
  # Get the schema and table info
  (schema, schema_table) = GetInfoSchemaAndTable(request, table)  
  
  # version_pending_log
  sql = "SELECT * FROM `version_pending_log` WHERE schema_id = %s AND schema_table_id = %s AND record_id = %s ORDER BY id"
  result_changelist = connection.Query(sql, [schema['id'], schema_table['id'], record_id])
  
  # version_commit_log
  sql = "SELECT * FROM `version_commit_log` WHERE schema_id = %s AND schema_table_id = %s AND record_id = %s ORDER BY id"
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
    
    data = utility.path.LoadYamlFromString(working['data_yaml'])
    
    if schema['id'] in data:
      if schema_table['id'] in data[schema['id']]:
        if record_id in data[schema['id']][schema_table['id']]:
          item = {'id':'working', 'name':'Working Version: %s' % user['name']}
          result.append(item)
  
  return result


def CommitWorkingVersion(request):
  """Immediately takes everything in this user's Working Set and creates a change list.
  
  This is the short-cut for Change Management, so we don't need all the steps and still have versionining.
  
  See also: CreateChangeList() and CreateChangeListFromWorkingSet() and CommitWorkingVersionSingleRecord()
  """
  working_version = GetUserVersionWorkingRecord(request)
  
  # Make a single record entry in the version_pending table, do all the work as we normally would (increments the PKEY, etc)
  version_number = CreateChangeList(request, working_version)
  
  # "commit" the changelist into version_commit, which will also put the data into the direct DB tables
  result = CommitChangeList(request, version_number)
  
  # Clean up the working version
  #TODO(g): Add commit=False to all of these and then Commit() after all of this?  Yes, do it when coming back to this.
  #   We need a CloneRequest() type method to get a new request connection object, so we are commiting the version data separately from the actual data.
  Delete(request, 'version_working', working_version['id'])
  
  return result


def CommitWorkingVersionSingleRecord(request, table, record_id):
  """TODO: Commit only a single record out of the working version.  Not all the working version records...
  
  Immediately takes a working version record and commits it, moving it through the rest of change management.
  
  This is the short-cut for Change Management, so we don't need all the steps and still have versionining.
  
  See also: CreateChangeList() and CreateChangeListFromWorkingSet()
  """
  raise Exception('TBD...')

  working_version = GetUserVersionWorkingRecord(request)
  
  record = GetRecordFromVersionRecord(request, working_version, table, record_id)

  #TODO(g): Format the record so it is wrapped in it's schema/schema_table/record dicts, as it is a full change now
  pass
  wrapped_record = None
  pass
  
  # Make a single record entry in the version_pending table, do all the work as we normally would (increments the PKEY, etc)
  version_number = CreateChangeList(request, wrapped_record)
  
  # "commit" the changelist into version_commit, which will also put the data into the direct DB tables
  result = CommitChangeList(request, version_number)
  
  # Clean up the working version
  raise Exception('Clean up the working version, we just left that there after making the Change List and commiting it...  Also, transactions?  Make sure thats OK too.  Commit on end?  New request for versions?  Clone request to get new connection/transaction?  Yes.')
  
  return result


def CommitChangeList(request, version_number):
  """Commit a pending change list (version_pending) to the final data, and put in version_commit.
  
  Also updates version_pending_log (removes entries), and version_commit_log (adds entries).
  
  This function works as a single datasbase transaction, so it cannot leave the data in an inconsistent state.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    version_number: int, this is the version number in the version_pending.id
  
  Returns: None
  """
  # Get the specified change list record
  record = Get(request, 'version_pending', version_number, use_working_version=False)
  
  # Create the new commit record to be inserted
  data = {'user_id':request.user['id'], 'data_yaml':record['data_yaml']}
  
  # Insert into version_commit
  version_commit_id = SetDirect(request, 'version_commit', data, commit=False)
  
  # Create the commit_log data
  CreateVersionLogRecords(request, 'version_commit', version_commit_id, data, commit=False)
  
  # Remove the version_pending_log row
  DeleteFilter(request, 'version_pending_log', {'version_pending_id':version_number}, commit=False)
  
  # Remove the version_pending row
  Delete(request, 'version_pending', record['id'], commit=False)
  
  # Make the change to the tables that are effected
  CommitVersionRecordToDatasource(request, version_commit_id, record, commit=False)
  
  # Commit the request
  Commit(request)


def CommitVersionRecordToDatasource(request, version_commit_id, change_record, commit=True):
  """Commit a change from the version_commit table into the real (non-versioning) datasource tables.
  
  This should not be called from outside this library, mostly because there is no reason to and it
  requires correct internal state to keep things in order.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    version_number: int, this is the version number in the version_pending.id
    change_record: dict, `version_commit` table row data
    commit: boolean (default True), if True any queries that could be commited will be (single query transaction), if False then a later Commit() will be required
  
  Returns: None
  """
  # Parse the YAML for our cahnge data
  update_data = utility.path.LoadYamlFromString(change_record['data_yaml'], {})
  delete_data = utility.path.LoadYamlFromString(change_record['delete_data_yaml'], {})
  admin_data = utility.path.LoadYamlFromString(change_record['admin_data_yaml'], {})
  rollback_data = utility.path.LoadYamlFromString(change_record['rollback_data_yaml'], {})
  
  
  # Get all our items as a flat dict of our changes, as it's easier to iterate over
  update_items = data_control.NestedDictsToSingleDict(update_data, 3)
  
  
  # Track dependencies here
  dependencies = {}
  dependency_results = {}
  dependency_update = {}
  
  # We start out with deferred items being everything, then we remove them, as possible, and loop until they are clear
  deferred_items = dict(update_items)
  
  # Create sorted record_keys list, we know these as they clear their dependencies, so automatically sorted
  sorted_items = []
  
  # We will keep looping until we either have no deferred items, or we couldnt find any more dependencies (circular references)
  change_occurred = True
  while deferred_items and change_occurred:
    # This must be set to True each time, or we will not loop again, and if there are still deferred items, we will error
    change_occurred = False
    
    # Loop over our update_items and create a dependency structure
    deferred_keys = deferred_items.keys()
    for record_key in deferred_keys:
      (schema_id, schema_table_id, record_id) = record_key
      (schema, schema_table) = GetInfoSchemaAndTableById(request, schema_table_id)
      record_data = deferred_items[record_key]
      
      # If we are a new record (id<0), add ourselves to the dependencies dict
      if record_data.get('id', 0) < 0:
        if schema_table['name'] not in dependencies:
          dependencies[schema_table['name']] = {}
        
        # Create our entry in the dependencies, so we can be easily referenced against fields of dependent records
        dependencies[schema_table['name']][record_data['id']] = record_key
      
      
      # Test the fields for dependencies first
      field_dependencies = {}
      
      # Check if this has any field dependencies
      for (field, field_value) in record_data.items():
        if field.endswith('_id') and type(field_value) == int and field_value < 0:
          # Get the table name (potentially), by stripping off the '_id' characters
          table_name = field[:-3]
          
          # Ensure this is actually a table name we know about
          try:
            # If this doesnt throw an exception, this is a real table name, and so this is a join table, and we should add it as a dependency
            field_schema, field_schema_table = GetInfoSchemaAndTable(request, table_name)
          except Exception, e:
            # This is not a table name, so we dont need to track it as a dependency, skip this field
            continue
          
          #TODO(t): this should really look at foreign keys (or the equivalent)-- but we'll do this for now
          # Loop over all remaining keys, if the table we are referencing is in the remaining list we 
          # cannot go now, so we'll add the field dep
          for k in deferred_keys:
            if k == record_key:
              continue
            if field_schema['id'] == schema_id and k[1] == field_schema_table['id']:
              field_dependencies[table_name] = field_value
          
          # Check to see if we already have the dependency we need listed
          if table_name in dependencies and field_value in dependencies[table_name]:
            # If we already have it, do nothing.  We will see this doesnt have any dependent fields (if it doesnt), and remove it from the deferred queue after this block
            if record_key not in dependency_update:
              dependency_update[record_key] = {}
            
            # Save what we want to collect once we are ready to be inserted, later (after our required records are inserted)
            dependency_update[record_key][field] = {'table': table_name, 'value': field_value}
            # print 'Recording lookup for later: %s: %s: %s: %s' % (record_key, field, table_name, field_value)
          
      
      # If we dont have any field dependencies, then we can be removed from the deferred items, as we are set
      if not field_dependencies:
        # print 'No dependencies, so removing: %s' % str(record_key)
        del deferred_items[record_key]
        
        # We modified the deferred items, so change has occurred
        change_occurred = True
        
        # We can sort these items on the order they clear being deferred, this guarantees we know their dependencies already, so is perfect
        sorted_items.append(record_key)
  
  
  # print '\n\n::: Dependencies:\n%s\n\n' % pprint.pformat(dependencies)
  # print '\n\n::: Dependency Updates:\n%s\n\n' % pprint.pformat(dependency_update)
  # print '\n\n::: Sorted Items:\n%s\n\n' % pprint.pformat(sorted_items)
  
  # If we had a circular reference, we still have deferred items, but we couldnt change anything in a pass, so we are stuck
  if not change_occurred and deferred_items:
    raise Exception('Circular reference found in deferred items, could not create dependency list: %s' % deferred_items)

  
  # Process all our sorted items, which takes care of getting dependencies when needed
  for record_key in sorted_items:
    (schema_id, schema_table_id, record_id) = record_key
    (schema, schema_table) = GetInfoSchemaAndTableById(request, schema_table_id)
    record = update_items[record_key]

    # If this is a New Record (id<0), remove the 'id' field, as we will auto_increment it into existance and uet the updated_record_id from that
    if record.get('id', 0) < 0:
      del record['id']
    
    # Rollback: Get the real record, this is None if it doesnt exist, which is also what we want.  It works for the positive and the negative existance cases.
    real_record = Get(request, schema_table['name'], record_id)
    data_control.EnsureNestedDictsExist(rollback_data, [schema_id, schema_table_id, record_id], real_record)
    
    # If this record requires any updates
    if record_key in dependency_update:
      for (field, field_data) in dependency_update[record_key].items():
        # Get our dependency record_key, we have to look it up
        dependency_record_key = dependencies[field_data['table']][field_data['value']]
        
        # Get our updated value
        record[field] = dependency_results[dependency_record_key]
    
    # If we got a real record, then overly our changes onto it, so we can save them properly
    if real_record:
      new_record = dict(real_record)
      new_record.update(record)
      record = new_record
    
    # Directly save this into table it was intended to be in.  Update it's own ID, so we convert New Records into existing ones
    # print '\nTODO: Set Direct: %s: %s\n' % (schema_table['name'], record)
    record['id'] = SetDirect(request, schema_table['name'], record, commit=commit)
    
    # Save all the updated record IDs here, if we need them, we will come and get them
    dependency_results[record_key] = record['id']
    
    # Put this record back into the update_data section, so that we keep the updated IDs, if they have been
    update_data[schema_id][schema_table_id][record_id] = record
  
  
  # Delete the specified records as well
  for (schema_id, schema_data) in delete_data.items():
    for (schema_table_id, schema_table_data) in schema_data.items():
      (schema, schema_table) = GetInfoSchemaAndTableById(request, schema_table_id)
      
      for record_id in schema_table_data:
        # Get the existing record, so we can store it for rollback
        real_record = Get(request, schema_table['name'], record_id)
        data_control.EnsureNestedDictsExist(rollback_data, [schema_id, schema_table_id, record_id], real_record)
        
        # Delete the Real record
        Delete(request, schema_table['name'], record_id)

  print '\n\n::: Roll Back data:\n%s\n\n' % pprint.pformat(rollback_data)
  
  # Save the updated version_commit results back into the DB, with our rollback data and any updated fields from dependencies
  change_record['data_yaml'] = utility.path.DumpYamlAsString(update_data)
  change_record['rollback_data_yaml'] = utility.path.DumpYamlAsString(rollback_data)
  SetDirect(request, 'version_commit', change_record)


def CreateVersionLogRecords(request, version_table, version_id, data, commit=True):
  """Create all the rows needed in the `version_*_log` tables (specified by table) for the data
  
  Version table information to stored in data['data_yaml'] as JSON encoded dict, which is keyed
  on the schema.id (int), then a dict keyed on the schema_table.id (int), then the field names (string)
  for the final dict, with values of the table field values (varying types).
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    version_table: string, name of table to operate on
    version_id: int, the PKEY of the table record we are creating log records for
    data: dict, record that we want to store in the table row
    commit: boolean (default True), if True any queries that could be commited will be (single query transaction), if False then a later Commit() will be required
  
  Returns: list of ints, all of the PKEYs for the log rows we inserted
  """
  # We will return a list of ints, which are all the row `id` field values (PKEYs) for the table records
  log_row_ids = []
  
  
  Log('Change Log: %s' % data, logging.DEBUG)
  
  change = utility.path.LoadYamlFromString(data['data_yaml'])
  
  Log('Writing version log records for: %s' % change, logging.DEBUG)
  
  # Process all the schema table fields we need version logs for
  for (schema_id, schema_tables) in change.items():
    
    Log('Change: %s : %s' % (schema_id, schema_tables))
    
    for (schema_table_id, records) in schema_tables.items():
      for record_id in records:
        # Create our reference field, based on the table name (ex: version_commit_id)
        reference_field = '%s_id' % version_table
        version_table_log = '%s_log' % version_table
        
        # Create the log record data to insert
        log_data = {reference_field: version_id, 'schema_id':schema_id, 'schema_table_id':schema_table_id, 'record_id':record_id}
        
        # Directly save this into the `version_*_log` table, with the commit flag specified
        SetDirect(request, version_table_log, log_data, commit=commit)
  
  return log_row_ids


def CreateChangeList(request, data):
  """Create a change list from the given table and record_id from the Working Set.
  
  This ensures we always have at least something in a change list, so we dont end up with empty ones where we dont know what
  went wrong.  We will always have some indication of what was going on if we find a change list.
  
  See also: CommitWorkingVersion() and CreateChangeListFromWorkingSet()
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    data: dict, record that we want to store in the table row
  
  Returns: int, version_number for this change list
  """
  Log('Create change list: %s' % data)
  
  # data_yaml = utility.path.DumpYamlAsString(data)
  
  # Create the changelist record
  record = {'user_id':request.user['id'], 'data_yaml':data['data_yaml']}
  
  # Create this version pending change, and get the version number
  version_number = SetDirect(request, 'version_pending', record)
  
  # Create the version log for this pending change
  CreateVersionLogRecords(request, 'version_pending', version_number, record)
  
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
  
  Returns: boolean, True if there was a working version to abandon, False if there was no working version to abandon
  """
  # Get this user's working version record
  try:
    record = GetUserVersionWorkingRecord(request)
  
  except datasource.VersionNotFound, e:
    return False
  
  
  # Extract the data_yaml payload
  change = utility.path.LoadYamlFromString(record['data_yaml'])

  # Format record key
  #TODO(g): Do this properly with the above dynamic PKEY info.  Is this good enough because we take record_id?  Maybe this needs to already be turned into the data_key?  This definitely needs to be a First Class Citizen in schemaman
  data_key = record_id
  
  # Get a connection
  connection = GetConnection(request)
  
  # Get the schema and table info
  (schema, schema_table) = GetInfoSchemaAndTable(request, table)


  # Add this set data to the version change record, if it doesnt exist
  if schema['id'] not in change:
    raise Exception('This user does not have the specified record in their working version: %s: %s: %s: %s: %s' % (request.username, table, schema['id'], schema_table['id'], record_id))
  
  # Add this table to the change record, if it doesnt exist
  if schema_table['id'] not in change[schema['id']]:
    raise Exception('This user does not have the specified record in their working version: %s: %s: %s: %s: %s' % (request.username, table, schema['id'], schema_table['id'], record_id))
  
  # Delete the record from the working version data
  del change[schema['id']][schema_table['id']]
  
  # Put this change record back into the version_change table, so it's saved
  record['data_yaml'] = utility.path.DumpYamlAsString(change)

  
  # Save the change record
  result_record = SetDirect(request, 'version_working', record)
  
  return True


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
  
  # Get the schema and table info
  (schema, schema_table) = GetInfoSchemaAndTable(request, 'version_working')  
  
  # Get the current working record for this user (if  any)
  sql = "SELECT * FROM `version_working` WHERE `user_id` = %s"
  result = connection.Query(sql, [user_id])
  
  # If we have a current change version record, use that
  if result:
    record = result[0]
  
  # Else, we dont have one yet, so create one
  else:
    raise datasource.VersionNotFound('No version working data exists for user: %s' % request.username)
  
  return record


def GetRecordFromVersionRecord(request, version_record, table, record_id):
  """Get a record from a version record (could be version_working, changelist or commit row record).
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    table: string, name of table to operate on
    record_id: int, record `id` field, primary key
    
  Returns: dict, row record from the version record (stored in data_yaml field)
  """
  # Get the JSON payload from the version record
  change = utility.path.LoadYamlFromString(version_record['data_yaml'])
  
  Log('GetRecordFromVersionRecord: Change: %s' % change, logging.DEBUG)
  
  # Format record key
  #TODO(g): Do this properly with the above dynamic PKEY info
  data_key = record_id
  
  # Get the schema and table info
  (schema, schema_table) = GetInfoSchemaAndTable(request, table)
  
  # Add this set data to the version change record, if it doesnt exist
  if schema['id'] not in change:
    raise datasource.RecordNotFound('Could not find version record: %s: %s: %s: %s: %s' % (request.username, table, schema['id'], schema_table['id'], record_id))
  
  # Add this table to the change record, if it doesnt exist
  if schema_table['id'] not in change[schema['id']]:
    raise datasource.RecordNotFound('Could not find version record: %s: %s: %s: %s' % (request.username, table, schema['id'], schema_table['id'], record_id))
  
  # Ensure the record is in the table
  if data_key not in change[schema['id']][schema_table['id']]:
    raise datasource.RecordNotFound('Could not find version record: %s: %s: %s: %s: %s' % (request.username, table, schema['id'], schema_table['id'], record_id))
  
  return change[schema['id']][schema_table['id']][data_key]
  

def Commit(request):
  """Commit a datasource transaction that is in the middle of a transaction.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
  
  Returns: None
  """
  connection = GetConnection(request)
  
  connection.Commit()


def AbandonCommit(request):
  """Abandon Commit a datasource transaction that is in the middle of a transaction.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
  
  Returns: None
  """
  connection = GetConnection(request)
  
  connection.AbandonCommit()


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
    print sql
    result = connection.Query(sql, values, commit=commit)
    
    # If we did an Update, we really want the 'id' field returned, like INSERT does (consistency and not having to do this all the time after an update)
    if result == 0 and update_returns_id:
      # If we have a primary 'id' key, use that
      if 'id' in data:
        result = Get(request, table, data['id'])
      
      # Else, use all the fields to get it explicitly
      else:
        # This should always return a single dict in a list, due to our uniqueness constraints
        rows = Filter(request, table, data)
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
  
  found_version_record = None
  
  # If we want to use the working version, lets get the data
  if use_working_version and not version_number:
    # Get the schema and table info
    (schema, schema_table) = GetInfoSchemaAndTable(request, table)
    
    try:
      working_version = GetUserVersionWorkingRecord(request)

      working_data = utility.path.LoadYamlFromString(working_version['data_yaml'])

      #TODO(g): Also handle deletes
      pass
      
      # If we have the working data (!None), and this scheme ID is in it, then look deeper
      if working_data and schema['id'] in working_data:
        db_data = working_data[schema['id']]
        if schema_table['id'] in db_data:
          table_data = db_data[schema_table['id']]

          # If we have this record_id, in this table, in this database, then return the working record
          if record_id in table_data:
            # print '\n\nFound Working Record: %s\n\n' % table_data[record_id]
            
            # Update this data over the existing table data
            found_version_record = table_data[record_id]
            
            # Ensure it has a record ID.  We remove this from the data, since it doesnt change, and it needs to be added back on these transition points
            found_version_record['id'] = record_id
    
    except datasource.VersionNotFound, e:
      pass
  
  # Else, if they want to retrieve a specified version number
  elif version_number:
    # Look in the pending table first
    version_record = Get(request, 'version_pending', version_number)
    
    print 'Found pending version: %s' % version_record
    
    # If we didnt find it in pending, check in committed
    if not version_record:
      version_record = Get(request, 'version_commit', version_number)
      
      print 'Found committed version: %s' % version_record
    
      # If we didnt find it in commited, error
      if not version_record:
        raise RecordNotFound('Couldnt find version record: Table: %s:  Version: %s  Record ID: %s' % (table, version_number, record_id))
    
    
    # Get the schema and table info
    (schema, schema_table) = GetInfoSchemaAndTable(request, table)
    
    rollback_record_data = utility.path.LoadYamlFromString(version_record['rollback_data_yaml'], {})
    update_record_data = utility.path.LoadYamlFromString(version_record['data_yaml'], {})
    
    # Layer the rollback data (if any), underneath the update data
    record_data = rollback_record_data
    record_data.update(update_record_data)
    
    #TODO(g): Also handle deletes
    pass
    
    # If we have the working data (!None), and this scheme ID is in it, then look deeper
    if schema['id'] in record_data:
      db_data = record_data[schema['id']]
      if schema_table['id'] in db_data:
        table_data = db_data[schema_table['id']]

        # If we have this record_id, in this table, in this database, then return the working record
        if record_id in table_data:
          # Update this data over the existing table data
          found_version_record = table_data[record_id]
          
          # Ensure it has a record ID.  We remove this from the data, since it doesnt change, and it needs to be added back on these transition points
          found_version_record['id'] = record_id
    #TODO(g):REMOVE: Once we are sure this code is stable and we dont need it.  Lots of lines, and I think things are fine...
    #       
    #       print 'Found version record: %s' % found_version_record
    #     else:
    #       print 'Couldnt find record ID in version record: %s' % record_id
    #   else:
    #     print 'Couldnt find schema table ID in version record: %s' % schema_table['id']
    # else:
    #   print 'Couldnt find schema ID in version record: %s (%s)' % (schema['id'], type(schema['id']))
  
  
  #TODO(g): Confirm this is the primary key name, not just "id" all the time.  Can look this up in our schema_data_paths from connection_data...
  #TODO(g): Allow multiple fields for primary key, and do the right thing with them
  sql = "SELECT * FROM `%s` WHERE id = %s" % (table, int(record_id))
  result = connection.Query(sql)
  
  if result:
    record = result[0]
    
    # If we also found a version record, overlay it
    if found_version_record:
      record.update(found_version_record)
      # print 'Found Version Record, updating over Real Record:  Returning: %s (%s): %s' % (record_id, type(record_id), record)
    
  else:
    record = None
    
    # If we found a version record, use it.
    #TODO(g): In this case, every field should be present, or it will be a "corrupt" record.  How to test this?  Do I need to?  Maybe...  Optional not to allow this?  Not sure.  Thing about it...
    if found_version_record:
      record = found_version_record
   
      # print 'Couldnt find Real Record, but Found Version Record:  Returning: %s (%s): %s' % (record_id, type(record_id), record)
 
  return record


def Query(request, sql, params=None):
  """Perform a query without versioning.
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
    sql: string, SQL query to perform
    params: list of values, any value type will inserted into the query to be quoted properly
    version_number: int (default None), if an int, this is the version number in the version_change or version_commit
        tables.  version_change is scanned before version_commit, as these are more likely to be requested.
    use_working_version: boolean (default True), if True and version_number==None this will also look at any
        version_working data and return it instead the head table data, if it exists for this user.
  
  Returns: dict, single record key/values
  """
  # Get a connection
  connection = GetConnection(request)
  
  # Query
  result = connection.Query(sql, params)
  
  return result


def Filter(request, table, data=None, use_working_version=False, order_list=None, groupby_list=None, order_ascending=True, version_number=None, limit=None, row_offset=None):
  """Get 0 or more records from the datasource, based on filtering rules.  Works against a single table.
  """
  
  # print '\nFilter: %s: %s' % (table, data)
  
  if not data:
    data = {}
  
  base_sql = "SELECT * FROM `%s` WHERE %s %s %s"
  
  keys = data.keys()
  keys.sort()
  
  keys_ticked = []
  where_list = []
  values = []
  
  # Order By
  if order_list:
    order_by = ' ORDER BY %s' % ', '.join(('`'+item+'`' for item in order_list))
    if order_ascending:
      order_by += ' ASC'
    else:
      order_by += ' DESC'
  else:
    order_by = ''
  
  # Group By
  if groupby_list:
    group_by = ' GROUP BY %s' % ', '.join(groupby_list)
  else:
    group_by = ''
  
  
  # Get our backticked wrapped insert keys, our value list, and our update setting
  for count in range(0, len(keys)):
    # Skip any fields that are NULL.  They are not helping us here, and cause problems with "=" vs "IS", because SQL implements NULL testing stupidly
    if data[keys[count]] == None:
      continue
    
    # Back tick column names
    ticked_key = '`%s`' % keys[count]
    keys_ticked.append(ticked_key)
    
    # If this is a normal value.  All non-normal tests should be wrapped in tuple (not other sequences) for the proper magic to occur.
    if type(data[keys[count]]) not in (tuple, list):
      # Update keys will reference the insert keys, so we dont have to specify the data twice (SQL does it)
      #TODO(g): Should I remove the primary key from this?  Not sure it's necessary.  Remove comment when proven it works without removing it (simpler)...
      where_list.append('%s = %%s' % ticked_key)
      
      # Values are passed in separate than the SQL string
      values.append(data[keys[count]])
    
    # Else, we want to do something being 
    else:
      #TODO(g): Do other op-codes too, so we can do many kinds of queries easily in this way
      pass
      
      # If the field is 'IN' a list of values
      if data[keys[count]][0].upper() == 'IN':
        match_list = data[keys[count]][1]
        where_in_str = '(%s)' % ', '.join(str(x) for x in match_list)

        # Set the full statement here, which means we have to handle quoting the strings ourselves, if VARCHAR-like type
        where_list.append('%s IN %s' % (ticked_key, where_in_str))

      # If the field is 'IS' a list of values
      elif data[keys[count]][0].upper() == 'IS':
        # Just join all the terms: IS NULL, IS NOT NULL, IS IN, IS NOT IN, it doesnt matter as they all work out
        where_in_str = ' '.join(data[keys[count]])
        where_list.append('%s %s' % (ticked_key, where_in_str))

        
      else:
        raise Exception('Filter: Unknown WHERE directive: %s' % data[keys[count]])
  
  
  # Build out strings to insert into our base_sql
  where_sql = ' AND '.join(where_list)
  
  # Create our final SQL, if we had WHERE list items
  if where_list:
    sql = base_sql % (table, where_sql, order_by, group_by)
  
  # Else, get all the records
  else:
    sql = "SELECT * FROM `%s` %s %s" % (table, order_by, group_by)
  
  
  # If limit rows
  if limit:
    # If we have a row offset, allow that too
    if row_offset:
      sql  += ' LIMIT %s,%s' % (row_offset, row_offset + limit)
    else:
      sql  += ' LIMIT %s' % limit
  
  # Log('\n\nGetFromData: %s: %s\nSQL:%s\nValues:%s\n' % (table, data, sql, values))
  
  
  # Get a connection
  connection = GetConnection(request)
  
  # Query
  rows = connection.Query(sql, values)
  
  
  # Assume we have no version data
  (update_version, delete_version) = (None, None)
  
  # If we want to use the working version, and we havent specified a version number (we dont want to mix both, too confusing.  Version Number is more explicit, it wins)
  #TODO(g): Move this section to generic_handler.py, because it can be generalized to all DB Handlers.
  if use_working_version and version_number == None:
    # Get the working version data for this user
    (update_version, delete_version) = GetWorkingVersionData(request)
    
    # print 'Version Record: Working: %s: \nUpdate: %s\nDelete: %s\n' % (is_pending, update_version, delete_version)


  # If we have specified an explicit version number, get it and see if it 
  if version_number:
    (version_record, is_pending) = GetInfoVersionNumber(request, version_number)
    update_version = utility.path.LoadYamlFromString(version_record['data_yaml'], {})
    delete_version = utility.path.LoadYamlFromString(version_record['delete_data_yaml'], {})
    
    print 'Version Record: Pending: %s: \nUpdate: %s\nDelete: %s\n' % (is_pending, update_version, delete_version)
  
  
  # If we have either update or deletes, from working, pending or committed versions.  We handle them all the same way.
  if update_version or delete_version:
    (schema, schema_table) = GetInfoSchemaAndTable(request, table)
    
    # Ensure rows is a list (mutable)
    if type(rows) == tuple:
      rows = list(rows)
    
    # Look to see if we have an Updates from our Working Version data, to make changes to the rows
    if schema['id'] in update_version:
      update_schema = update_version[schema['id']]
      
      if schema_table['id'] in update_schema:
        update_table = update_schema[schema_table['id']]
        
        # Get a list of all the row IDs, so we can see if we need to add any
        row_id_list = []
        
        # Loop over our row results
        for row in rows:
          # Add the row ID, so we know them all
          row_id_list.append(row['id'])
          
          # If the row we got from Filter() exists in our update_table, update those contents over the row
          if row['id'] in update_table:
            row.update(update_table[row['id']])
        
        # Loop over the update_table, and see if we have any entries we dont have in the rows, but that meet the requirement
        for (item_key, item) in update_table.items():
          # Set the ID field.  We remove it when putting it into the working table, because it doesnt change, so we have to add it back when creating records from that that table
          item['id'] = item_key
          
          # If this is a potential match
          if item['id'] not in row_id_list:
            
            # print 'Found potential match: %s' % item
            
            #TODO(t): this shouldn't live right here-- as I imagine we'll need to call this from multiple places
            def check_filter(item, filter_key, filter_value):
              """Return whether the item's filter_key matches the value definition of filter_value
              """
              if filter_key not in item:
                return False
              # If this is just a normal value, then we just need to compare
              if type(filter_value) not in (tuple, list):
                return item[filter_key] == filter_value
              else:
                # If the field is 'IN' a list of values
                if filter_value[0].upper() == 'IN':
                  match_list = filter_value[1]
                  return item[filter_key] in match_list
                else:
                  #TODO(t): this should implement the other checks (such as IS)
                  raise NotImplementedError('Filter does not support filter_value of %s' % filter_value)
              return True
            
            # Check if any of the filter key-values dont match, we only want to add it if they all match
            filter_data_matched = True
            for (filter_key, filter_value) in data.items():
              if not check_filter(item, filter_key, filter_value):
                filter_data_matched = False
                # print '  Not matched: %s != %s' % (item.get(filter_key, '*KEY NOT FOUND*'), filter_value)
                break
            
            # If all the conditions are met
            if filter_data_matched:
              # Add this record to the rows.
              rows.append(item)
              
              #TODO(g): Sort these.  Order By, Group By, etc.
              pass
        
        #TODO(g): Order by, group by, etc.  We can control ALL the data so it's perfectly integrated, and looks like its part of the query
        pass
      
    # If we want these ordered, we need to sort them again
    if order_list:
      # Sort the rows by the order_list, so we have an ordered return set again
      rows = datasource.SortRows(rows, order_list)
    
    
    # print '\n\n+++ Delete version: %s' % delete_version
    
    # If we have any entries that we might need to delete, in our working version (delete versions)
    if schema['id'] in delete_version:
      delete_schema = delete_version[schema['id']]
      
      if schema_table['id'] in delete_schema:
        delete_table = delete_schema[schema_table['id']]
        
        # print '\n\n-*- Found entry in Delete Version table while in Filter: %s' % delete_table
        
        delete_rows = []
        
        # Add any rows matching our delete entry to the delete list
        for row in rows:
          if row['id'] in delete_table:
            # print 'Matched delete row: %s' % row
            delete_rows.append(row)
        
        # Remove any rows marked for deletion
        for row in delete_rows:
          # print 'Removing row: %s' % row
          rows.remove(row)



  return rows


def GetInfoVersionNumber(request, version_number):
  """Returns the version_* record information, for the specified version_number.
  
  First checks in version_pending, then checks version_commit.  It will only be in one or the other.
  
  Returns: tuple (dict (or None), boolean), if found, the (record, is_pending), else (None, is_pending)
  """
  record = Get(request, 'version_pending', version_number)
  is_pending = True
  
  # If it wasn't in pending, get it in committed (if it exists)
  if record == None:
    record = Get(request, 'version_commit', version_number)
    is_pending = False
  
  return (record, is_pending)


def GetWorkingVersionData(request, username=None):
  """Returns the version_working record's data_yaml, already parsed to Python data dict
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
  
  Returns: tuple of (dict, dict), which is (update_version, delete_version), respectively
  """
  # If one wasnt passed in, we use the requester
  if not username:
    username = request.username
  
  # Get the user_id
  user_list = Filter(request, 'user', {'name':username})
  
  if not user_list:
    return ({}, {})
  
  try:
    user = user_list[0]
    
    version_working_list = Filter(request, 'version_working', {'user_id': user['id']})
    version_working = version_working_list[0]
    
    update_version = utility.path.LoadYamlFromString(version_working['data_yaml'])
    delete_version = utility.path.LoadYamlFromString(version_working['delete_data_yaml'])
  
    # Ensure they are properly initialized
    if update_version == None:
      update_version = {}
    if delete_version == None:
      delete_version = {}
    
    return (update_version, delete_version)
  
  except Exception, e:
    print 'GetWorkingVersionData: Failed: %s: %s' % (username, e)
    return ({}, {})


def SetWorkingVersionData(request, working_version, delete_version=None):
  """Returns the version_working record's data_yaml, already parsed to Python data dict
  
  Args:
    request: Request Object, the connection spec data and user and auth info, etc
  
  Returns: tuple of (dict, dict), which is (update_version, delete_version), respectively
  """
  user = GetUser(request)
  
  # Get the current working record directly, if we have it
  working_list = Filter(request, 'version_working', {'user_id': user['id']})
  if working_list:
    record = working_list[0]
  else:
    record = {'user_id': user['id'], 'delete_data_yaml': None}
  
  
  # Convert to YAML, for storage
  record['data_yaml'] = utility.path.DumpYamlAsString(working_version)
  
  # If we specified delete, update that too
  if delete_version != None:
    record['delete_data_yaml'] = utility.path.DumpYamlAsString(delete_version)
  
  # Update the working record
  SetDirect(request, 'version_working', record)


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
  
  # Ensure they arent trying to truncate
  if not data:
    raise InvalidArguments('DeleteFilter requires a dict with values to filter on.  Truncation of all data is not allowed from this function with an empty dictionary.')
  
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


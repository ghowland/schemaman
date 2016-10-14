"""
Datasource: MySQL: Querying
"""


import threading
import logging

try:
  import mysql.connector
  from mysql.connector import errorcode
  MYSQL = 'ORACLE'
  
except ImportError, e:
  import MySQLdb
  import MySQLdb.cursors
  MYSQL = 'GHETTO'

from schemaman.utility.log import Log


# Default connection pool size.  Override with connection_data
DEFAULT_CONNECTION_POOL_SIZE = 20


# Connection Pool of Pools (servers on first level (list), pool of connections on second (Connection objects)): dict of lists of Connection objects
CONNECTION_POOL_POOL = {}
CONNECTION_POOL_POOL_LOCK = threading.Lock()


# Default MySQL connection character set
# DEFAULT_CHARSET = 'utf8'
DEFAULT_CHARSET = 'latin1'


# For every request, they can only take 1 request at a time, so they dont collide, so we lock to ensure they are sequential
REQUEST_QUERY_LOCK = {}


#TODO(g): Make this a base class, that each Handler type sub-classes.  Useful in this case, as it's an interface, and some methods are more virtual than others.  It's good to have a base class for the interface, otherwise every handler implements its own base, and they seem more disconnected...
class Connection:
  """This wraps MySQL connection and cursor objects, as well as tracks the progress of any requests, and if it is available for use by a new request."""
  
  def __init__(self, connection_data, server_id, request):
    Log('Creating new connection: MySQL: %s' % connection_data['datasource']['database'])
    
    # We need these to actually connect
    self.connection_data = connection_data
    self.server_id = server_id
    
    # This tells us who is connection.  Release() to make this connection available for other requests.
    self.request = request
    
    # Generate the server key, since this specifies which CONNECTION_POOL_POOL we are in
    self.server_key = '%s.%s' % (connection_data['alias'], server_id)
    
    self.connection = None
    self.cursor = None
    
    # Connect
    self.Connect()
  
  
  def __del__(self):
    try:
      self.Close()
    
    # Ignore this failure, this happens when the context is lost, because the program is closing, and we are working with NoneTypes, instead of expected types
    except AttributeError, e:
      pass


  def Close(self):
    """Close the cursor and connection, if they are open, and set them to None"""
    if self.cursor:
      try:
        self.cursor.close()
      finally:
        self.cursor = None
      
    if self.connection:
      try:
        self.connection.close()
      finally:      
        self.connection = None


  def Release(self):
    """Release this connection."""
    self.request = None


  def Acquire(self, request):
    """Acquire this Connection for this Request."""
    if self.request != None:
      raise Exception('Attempting to Acquire a Connection when it is already owned: %s' % request)
    
    self.request = request

  
  def IsAvailable(self):
    """Returns boolean, True if not currently being used by a request and has a non-None connection.
    
    NOTE(g): This does not verify the connection, just ensures that we think we have a valid connection.
    """
    if self.request and self.connection:
      return False
    
    else:
      return True
  

  def IsInUse(self):
    """Returns boolean, True if not currently being used by a request."""
    if self.request:
      return True
    
    else:
      return False


  def IsUsedByRequest(self, request):
    """Returns boolean, True if currently being used by the same request that is being passed in as request."""
    if self.request == request:
      return True
    
    else:
      return False
  
  
  def GetServerData(self):
    """Returns a dict for server data, which is layered from the server configs"""
    server_data = {}
    
    # Find the master host, which we will assume we are connecting to for now
    found_server = None
    for server_data in self.connection_data['datasource']['servers']:
      if server_data['id'] == self.server_id:
        found_server = server_data
        break
    
    if not found_server:
      raise Exception('Couldnt find ourselves in our server data, something is wrong: %s: %s' % (self.connection_data['alias'], self.server_id))
    
    # Set specific datasource fields, these fields may not be present, and are overridden by the server specific fields
    server_data['database'] = self.connection_data['datasource'].get('database', None)
    server_data['user'] = self.connection_data['datasource'].get('user', None)
    server_data['password_path'] = self.connection_data['datasource'].get('password_path', None)
    
    # Override all previous fields with server specific fields, which gives us the final data (most specific)
    server_data.update(found_server)
    
    return server_data
  
  
  def Connect(self):
    """Connect to the database"""
    server = self.GetServerData()
    
    Log('Connecting to MySQL server: %s: %s' % (server['host'], server['database']))
    
    # Read the password from the first line of the password file
    try:
      password = open(server['password_path']).read().split('\n', 1)[0]
      Log('Loaded password: %s' % server['password_path'])
      
    except Exception, e:
      Log('ERROR: Failed to read from password file: %s' % server['password_path'], logging.ERROR)
      password = None
    
    if MYSQL == 'ORACLE':
      self.connection = mysql.connector.connect(user=server['user'], password=password, host=server['host'], port=server['port'], database=server['database'], use_unicode=True, charset=DEFAULT_CHARSET)
      self.cursor = self.connection.cursor(dictionary=True)
    
    else:
      self.connection = MySQLdb.Connection(user=server['user'], passwd=password, host=server['host'], port=server['port'], db=server['database'])
      self.cursor = self.connection.cursor(cursorclass=MySQLdb.cursors.DictCursor)


  def Query(self, sql, params=None, commit=True):
    """Query the database via our connection."""
    set_request_lock = False
    if self.request:
      # Ensure any requests we look at, have a lock we can grab
      if self.request.request_number not in REQUEST_QUERY_LOCK:
        REQUEST_QUERY_LOCK[self.request.request_number] = threading.Lock()
      
      # Get the lock
      REQUEST_QUERY_LOCK[self.request.request_number].acquire()
      set_request_lock = True
        
    
    try:
      if not params:
        Log('Query: %s' % sql)
      else:
        Log('Query: %s -- %s' % (sql, params))
      
      result = Query(self.connection, self.cursor, sql, params=params, commit=commit)
    
    finally:
      if set_request_lock:
        REQUEST_QUERY_LOCK[self.request.request_number].release()
    
    return result
  
  
  def Commit(self):
    """Commit a transaction in flight."""
    result = self.connection.commit()
    
    return result


def MySQLReleaseConnections(request):
  """Release any connections tied with this request_number
  
  TODO(g): 
  """
  global CONNECTION_POOL_POOL
  
  # Generate the server key, since this specifies which CONNECTION_POOL_POOL we are in
  #TODO(g): Turn this into a function?  I have to duplicate this from the connection class otherwise...  Or only do it here?
  server_key = '%s.%s' % (request.connection_data['alias'], request.server_id)

  # Look through the current connection pool, to see if we already have a connection for this request_number
  if server_key in CONNECTION_POOL_POOL:
    for connection in CONNECTION_POOL_POOL[server_key]:
      # If this connection is for the same request, release it
      if connection.IsUsedByRequest(request):
        if MYSQL == 'ORACLE':
          connection.Release()
          
        else:
          connection.close()


def GetConnection(request, server_id=None):
  """Returns a connection to the specified database server_id, based on the request number (may already have a connection for that request)."""
  global CONNECTION_POOL_POOL
  
  # If we didnt have a server_id specified, use the master_server_id
  if server_id != None:
    server_id = server_id
  elif request.server_id == None:
    server_id = request.connection_data['datasource']['master_server_id']
  else:
    server_id = request.server_id
  
  
  # Find the master host, which we will assume we are connecting to for now
  found_server = None
  for server_data in request.connection_data['datasource']['servers']:
    if server_data['id'] == server_id:
      found_server = server_data
      break


  # Generate the server key, since this specifies which CONNECTION_POOL_POOL we are in
  #TODO(g): Turn this into a function?  I have to duplicate this from the connection class otherwise...  Or only do it here?
  server_key = '%s.%s' % (request.connection_data['alias'], server_id)


  # Look through the current connection pool, to see if we already have a connection for this request_number
  if server_key in CONNECTION_POOL_POOL:
    for connection in CONNECTION_POOL_POOL[server_key]:
      # If this connection is for the same request, use it
      if connection.IsUsedByRequest(request):
        return connection
  

  # Look through current connection pool, to see if we have any available connections in this server, that we can use
  if server_key in CONNECTION_POOL_POOL:
    for connection in CONNECTION_POOL_POOL[server_key]:
      # If this connection is available (not being used in a request)
      if connection.IsAvailable():
        #TODO(g): Make this a method to set it to this request
        connection.Acquire(request)
        return connection

  
  # Create the connection
  connection = Connection(request.connection_data, server_id, request)
  
  
  # Ensure we have a pool for this server in our connection pool pools
  if connection.server_key not in CONNECTION_POOL_POOL:
    try:
      CONNECTION_POOL_POOL_LOCK.acquire()
      Log('Creating new MySQL connection pool: %s' % connection.server_key)
      CONNECTION_POOL_POOL[connection.server_key] = [connection]
    finally:
      CONNECTION_POOL_POOL_LOCK.release()
  
  # Else, add it to the existing connection pool pool
  else:
    try:
      CONNECTION_POOL_POOL_LOCK.acquire()
      Log('Appending to existing MySQL connection pool: %s' % connection.server_key)
      CONNECTION_POOL_POOL[connection.server_key].append(connection)
    finally:
      CONNECTION_POOL_POOL_LOCK.release()
  
  return connection


def Query(conn, cursor, sql, params=None, commit=True):
  """Query"""
  cursor.execute(sql, params)
  
  if sql.upper().startswith('INSERT'):
    result = cursor.lastrowid
    
    # If we should immediately commit this as a 1 query transaction
    if commit:
      conn.commit()
      
  elif sql.upper().startswith('UPDATE') or sql.upper().startswith('DELETE'):
    # If we should immediately commit this as a 1 query transaction
    if commit:
      conn.commit()
    
    result = None
  
  elif sql.upper().startswith('SELECT') or sql.upper().startswith('SHOW') or sql.upper().startswith('DESC'):
    result = cursor.fetchall()
  
  else:
    result = None
  
  return result


def SanitizeSQL(text):
  if text == None:
    text = 'NULL'
  else:
    try:
      text = str(text)
      
    except UnicodeEncodeError, e:
      text = str(text.decode('ascii', 'ignore'))
  
  return text.replace("'", "''").replace('\\', '\\\\')


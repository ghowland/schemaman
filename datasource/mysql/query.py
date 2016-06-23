"""
Datasource: MySQL: Querying
"""


import threading

import mysql.connector


# Default connection pool size.  Override with connection_data
DEFAULT_CONNECTION_POOL_SIZE = 20


# Connection Pool of Pools (servers on first level (list), pool of connections on second (Connection objects)): dict of lists of Connection objects
CONNECTION_POOL_POOL = {}
CONNECTION_POOL_POOL_LOCK = threading.Lock()


class Connection:
	"""This wraps MySQL connection and cursor objects, as well as tracks the progress of any requests, and if it is available for use by a new request."""
	
	def __init__(self, connection_data, server_id, request_number=None):
		self.connection_data = connection_data
		self.server_id = server_id
		self.request_number = request_number
		
		# Generate the server key, since this specifies which CONNECTION_POOL_POOL we are in
		self.server_key = '%s.%s' % (connection_data['alias'], server_id)
		
		self.connection = None
		self.cursor = None
		
		# Connect
		self.Connect()
	
	
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
		
		print 'Connecting to MySQL server: %s: %s' % (server['host'], server['database'])
		
		# Read the password from the first line of the password file
		try:
			password = open(server['password_path']).read().split('\n', 1)[0]
			
		except Exception, e:
			print 'ERROR: Failed to read from password file: %s' % server['password_path']
			password = None
		
		self.connection = mysql.connector.connect(user=server['user'], password=password, host=server['host'], port=server['port'], database=server['database'])
		self.cursor = conn.cursor(cursor_class=MySQLCursorDict)
	
	
	def IsAvailable(self):
		"""Returns boolean, True if not currently being used by a request"""
		if self.request_number:
			return False
		
		else:
			return True
	

	def Query(self, sql, params=None):
		"""Query the database via our connection."""
		result = Query(self.connection, self.cursor, sql, params=params)
		
		return result


class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
	def _row_to_python(self, rowdata, desc=None):
		row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
		if row:
			return dict(zip(self.column_names, row))
		return None


def GetConnection(connection_data, request_number, server_id=None):
	"""Returns a connection to the specified database server_id, based on the request number (may already have a connection for that request)."""
	# If we didnt have a server_id specified, use the master_server_id
	if server_id == None:
		server_id = connection_data['datasource']['master_server_id']
	
	# Find the master host, which we will assume we are connecting to for now
	found_server = None
	for server_data in connection_data['datasource']['servers']:
		if server_data['id'] == server_id:
			found_server = server_data
			break

	
	# Create the connection
	connection = Connection(connection_data, server_id, request_number=request_number)
	
	
	# Ensure we have a pool for this server in our connection pool pools
	if server_key not in CONNECTION_POOL_POOL:
		try:
			CONNECTION_POOL_POOL_LOCK.acquire()
			print 'Creating new MySQL connection pool: %s' % connection.server_key
			CONNECTION_POOL_POOL[connection.server_key] = [connection]
		finally:
			CONNECTION_POOL_POOL_LOCK.release()
	
	# Else, add it to the existing connection pool pool
	else:
		try:
			CONNECTION_POOL_POOL_LOCK.acquire()
			print 'Appending to existing MySQL connection pool: %s' % connection.server_key
			CONNECTION_POOL_POOL[connection.server_key].append(connection)
		finally:
			CONNECTION_POOL_POOL_LOCK.release()
	
	return connection


def Query(conn, cursor, sql, params=None):
	cursor.execute(sql)
	
	if sql.upper().startswith('INSERT'):
		result = cursor.lastrowid
		conn.commit()
	elif sql.upper().startswith('UPDATE') or sql.upper().startswith('DELETE'):
		conn.commit()
		result = None
	elif sql.upper().startswith('SELECT'):
		result = cursor.fetchall()
	else:
		result = None
	
	cursor.close()
	conn.close()
	
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


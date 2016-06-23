"""
Datasource: MySQL: Querying
"""


import threading

import mysql.connector
from mysql.connector import errorcode


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
	
	
	def __del__(self):
		self.Close()


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
			print 'Loaded password: %s: %s' % (server['password_path'], password)
			
		except Exception, e:
			print 'ERROR: Failed to read from password file: %s' % server['password_path']
			password = None
		
		self.connection = mysql.connector.connect(user=server['user'], password=password, host=server['host'], port=server['port'], database=server['database'], use_unicode=True, charset='latin1')
		self.cursor = self.connection.cursor(dictionary=True)
	
	
	def IsAvailable(self):
		"""Returns boolean, True if not currently being used by a request and has a non-None connection.
		
		NOTE(g): This does not verify the connection, just ensures that we think we have a valid connection.
		"""
		if self.request_number and self.connection:
			return False
		
		else:
			return True
	

	def Query(self, sql, params=None):
		"""Query the database via our connection."""
		print 'Query: %s' % sql
		
		result = Query(self.connection, self.cursor, sql, params=params)
		
		return result


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
	if connection.server_key not in CONNECTION_POOL_POOL:
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


def Query(conn, cursor, sql, params=None, commit=True):
	"""Query"""
	cursor.execute(sql)
	
	if sql.upper().startswith('INSERT'):
		result = cursor.lastrowid
		
		if commet:
			conn.commit()
			
	elif sql.upper().startswith('UPDATE') or sql.upper().startswith('DELETE'):
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


"""
Request object to track user requests against our data sources.
"""


import threading


import generic_handler


# Every time we start, we use our initial global request counter, and on the beginning of a request, we increment it,
#   so that we can keep transactions together if they use the same request number.  Basically a singleton.
GLOBAL_REQUEST_COUNTER = 1
GLOBAL_REQUEST_COUNTER_LOCK = threading.Lock()


class Request:
  """Contains request information, and can close transactions due to scope GC collections."""
  
  def __init__(self, connection_data, username, authentication, request_number=None, server_id=None, use_version_management=True, auto_commit=False):
    self.connection_data = connection_data
    
    self.username = username
    self.authentication = authentication
    
    # Ensure we have a request number
    if not request_number:
      self.request_number = self.GetRequestNumber()
    
    else:
      self.request_number = request_number
    
    
    # If we have specified a specified server in a Connection Spec datasources, we can have it set here
    self.server_id = server_id
    
    # If True (default), then will use version management features, otherwise we just directly write to the Head of the datasource.
    #   Writing directly to head is necessary for all internal operations, which we do not want to bog down our system with
    #   automaticg version management rollback stuff.
    self.use_version_management = use_version_management
    
    # If True, we will auto commit any versions...
    self.auto_commit = auto_commit
    
    # Keep track of all handlers we use, so we can release the connections when we are done
    #TODO(g): Add handlers as we use them to this request, so we know what we need to release and dont have to try to scan the world...  Specific work is more efficient than scan-the-world work...
    self.datasource_handlers = []
    
    # Log is a list of tuples (text, data), 
    self.log = []
    
    
    # Get the user record
    #TODO(g): Where we get the user records needs to be configurable, and currently isnt.  Fix later, same with VMCM
    self.user = generic_handler.GetUser(self, self.username)
  
  
  def __del__(self):
    """Going out of scope, ensure we release any connections that we have."""
    try:
      self.ReleaseConnections()
    
    # Probably the interpretter is shutting down if we are getting this ("'NoneType' object is not callable"), so ignore it
    except TypeError, e:
      pass
  
  
  def Log(self, text, data):
    """Log any data we want to about this request."""
    self.log.append((text, data))
  
  
  def ReleaseConnections(self):
    """Release any connections we have open and tied to this request_number (wont close them)"""
    for handler in self.datasource_handlers:
      handler.ReleaseConnections(self.connection_data, self.request_number)


  def AddHandler(self, handler):
    """Allows us to track all handlers that might have datasource connections for us, so we can release them explicitly and efficiently."""
    if handler not in self.datasource_handlers:
      self.datasource_handlers.append(handler)


  def GetRequestNumber(self):
    """Returns an int, the next available request number.
    
    This number can be used with any number of data sets simultaneously, as it is globally unique, and
    each database connection will reside in it's own server's pool, so any number of databases
    can be queried with the same request_number.
    """
    global GLOBAL_REQUEST_COUNTER
    
    # Thread safe
    try:
      GLOBAL_REQUEST_COUNTER_LOCK.acquire()
      
      request_number = GLOBAL_REQUEST_COUNTER
      GLOBAL_REQUEST_COUNTER += 1
    
    finally:
      GLOBAL_REQUEST_COUNTER_LOCK.release()
    
    return GLOBAL_REQUEST_COUNTER
  

"""
Cache System for SchemaMan

Thread safe.  Locks around all gets and sets of cache data, and all creating of new cache items.

To keep this simple, there is no purge mechanism.  This keeps this code very simple and doesn't
require any maintenance time for cleanup, or extra function calls.

That means dont put long-tail things in this cache.  This is for core items that we may TTL out, but
we dont need to purge, because there arent millions of them.  Thousands of items in memory sitting
around is no problem.
"""


import threading
import time


# Create a pool for caching, and a lock for changing the pool (dicts are thread unsafe)
CACHE_POOL = {}
CACHE_POOL_LOCK = threading.Lock()

# Default Time-To-Live for our cache objects
DEFAULT_TTL = 60 * 5


class NoCacheResultFound:
  """This is used to store a 'no value' result, to differentiate from None, without throwing exceptions"""


class Cache:
  """Object for caching data for a given type."""
  
  def __init__(self, pool_key):
    self.pool_key = pool_key
    
    # We need to lock to get/set data safely
    self.lock = threading.Lock()
    
    # Our cache data, and timeout (time+TTL) for each cache item
    self.data = {}
    self.data_timeout = {}
  
  
  def Get(self, item_key, default_value=NoCacheResultFound):
    """Returns cached data, or default_value"""
    result = default_value
    
    # If our current time is less than the timeout time (time+TTL), if this item exists, get the item
    if time.time() < self.data_timeout.get(item_key, 0):
      result =  self.data.get(item_key, default_value)
    
    return result
  
  
  def Set(self, item_key, value, ttl=DEFAULT_TTL):
    """Sets cached data"""
    aquired_lock = False
    try:
      # Lock the cache and track it
      self.lock.acquire()
      aquired_lock = True
      
      # Set the cache data
      self.data[item_key] = value
      self.data_timeout[item_key] = time.time() + ttl
    
    # Ensure we always release our locks, no matter what
    finally:
      if aquired_lock:
        self.lock.release()


def GetCachePool(pool_key):
  """Returns a Cache class object, from our Cache pool.  Creates it if it doesnt exist."""
  global CACHE_POOL
  global CACHE_POOL_LOCK
  
  
  aquired_pool_lock = False
  try:
    # If we dont have this cache pool object, create it
    if pool_key not in CACHE_POOL:
      # Lock the pool and track it
      CACHE_POOL_LOCK.acquire()
      aquired_pool_lock = True
      
      # Create a Cache object, and put it in our pool key spot
      CACHE_POOL[pool_key] = Cache(pool_key)
  
  # Ensure we always release our locks, no matter what
  finally:
    if aquired_pool_lock:
      CACHE_POOL_LOCK.release()
  
  
  # Return the Cache object, we just enforced it exists
  return CACHE_POOL[pool_key]


def Get(pool_key, item_key, default_value=NoCacheResultFound):
  """Get a value from a cache pool"""
  cache = GetCachePool(pool_key)
  
  value = cache.Get(item_key, default_value=default_value)
  
  return value


def Set(pool_key, item_key, value, ttl=DEFAULT_TTL):
  """Set a value in cache pool"""
  cache = GetCachePool(pool_key)
  
  cache.Set(item_key, value, ttl=ttl)

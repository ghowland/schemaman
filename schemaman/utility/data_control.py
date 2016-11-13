"""
Perform general actions against data for SchemaMan

We need to have short cuts to do things on our data, because we repeat access patterns too frequently in code.
These are things like looping over nested data inside of dictionaries, and ensuring nested dictinary elements exist, etc.

TODO(g):
  - Multiple iterations in a single for loop.  Go down multiple keys, and assign all the variables each time, like it has been flattened.  This will remove a lot of lines of code where I'm iterating over schema/table/records
"""


def EnsureNestedDictsExist(container, key_list, default_value, depth=0):
  """Iterates down a set of sub-containers (dicts), using key_list to navigate, and ensures all of them exist.
  
  default_value will be assigned to the final key, if it does not already exist.  This could be anything.
  
  Args:
    container: dict, parent container at current call depth
    key_list: list of keys (any dict acceptable hash type), will start from first to last, going deeper, making dicts as needed
    default_value: any, this is assigned into the final key dict's value, if that final key doesnt already exist
    depth: int, used for tracking recusion.  Do not manually set.
  
  Returns: None, changes made inside container, if any
  """
  # Return if we have nothing to do (no keys)
  if not key_list:
    return
  
  # Get the current key (first one), remove it from our list, get the remaining key count
  current_key = key_list[0]
  key_list.remove(current_key)
  remaining_keys = len(key_list)
  
  # If this is the last key
  if not remaining_keys:
    # If we dont have this current_key, then set the default value, and we are done
    if current_key not in container:
      container[current_key] = default_value
      return
  
  # Else, if there are more keys
  else:
    # If we dont have this current key, then we create a new dictionary at that key position, since we are all about nested dicts
    if current_key not in container:
      container[current_key] = {}
    
    # Now that we know we have a dictionary at our current key, and we have more keys, we recurse
    #   The new container will be this current key position, the key_list has current_key removed so it shrunk, and we pass the rest through, tracking recursion
    EnsureNestedDictsExist(container[current_key], key_list, default_value, depth=depth+1)


def NestedDictsToSingleDict(container, nested_depth, key_list=None, depth=0, final_data=None):
  """Takes a nested dictionary, and used key_depth (int) of the keys to create a tuple, which it will
  then store all the child keys into a single flat dict.
  
  Args:
    container: dict, nested dict (contains more dicts as values)
    nested_depth: int, number of nesteded dict depth of keys to capture for tuple in result single dict
    key_list: None or list, do not use.  Meant for tracking the current key list, as we recurse
    depth: int, do not use.  Meant for tracking recusion
    final_data: None of dict, do not use.  Meant for passing data along, to create the result
  
  Returns: dict, singlar flat dict, keyed on tuple, tuple is key_depth elements long
  """
  # This is what we will return, as our flat dict, keyed by nested dict keys in a tuple, with nested_depth length
  if final_data == None:
    final_data = {}
  
  # We start with an empty list, which we use for our tuple key
  if key_list == None:
    key_list = []
  
  
  # Process the current depth in our nested dicts
  for (cur_key, cur_value) in container.items():
    # Create a new list from our incoming key_list, and the new current key
    cur_key_list = key_list + [cur_key]
    
    # If we are at the nested_depth that we want to capture data at, assign the value into this tuple
    if len(cur_key_list) == nested_depth:
      final_data[tuple(cur_key_list)] = cur_value
    
    # Else, we want to go deeper into the nested dicts, so recurse again
    else:
      # Process this current value now, which is our next Nested Dict.  No return value, as we modify final_data in place (side effect)
      NestedDictsToSingleDict(cur_value, nested_depth, key_list=cur_key_list, depth=depth+1, final_data=final_data)
  
  return final_data


def GetFromNestedDict(container, key_list, default_value=None):
  """Returns the value at the nested dict starting from container, going down each layer using the key_list"""
  current_container = container
  
  # Process all our keys
  for key in key_list:
    # If we dont have this key, return the default
    if key not in current_container:
      return default_value
    
    # Update the current container by moving into this key
    current_container = current_container[key]
  
  # If we are at the end of our key_list, this is the value we were looking for, whatever it is
  return current_container


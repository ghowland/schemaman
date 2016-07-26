"""
Format data for various data source operations.  Pre-query or post-query.
"""


def FormatStringOrNullFromDict(data, key, strict=False):
  """Returns a quoted and escaped string from the data[key] value, or NULL to insert into a field column
  
  If strict=True, then an exception is thrown if the key is missing, otherwise it is NULL.
  """
  if key not in data and strict:
    raise Exception('Missing data key: %s' % key)
  
  if key in data and data[key] != None:
    output = "'%s'" % SanitizeSQL(str(data[key]))
  else:
    output = 'NULL'
  
  return output


def FormatIntOrNullFromDict(data, key, strict=False):
  """Returns a quoted and escaped string from the data[key] value, or NULL to insert into a field column
  
  If strict=True, then an exception is thrown if the key is missing, otherwise it is NULL.
  """
  if key not in data and strict:
    raise Exception('Missing data key: %s' % key)
  
  if key in data and data[key] != None:
    output = "%s" % int(data[key])
  else:
    output = 'NULL'
  
  return output


def ConvertListToDict(list_data, key_field, multi_key_separtor='.'):
  """Returns a dict keyed on key_field of list_data item dict data.  list_data is a list of dicts.
  
  key_field can be string (single value) or list (combined value into a key.field.value dict key)
  """
  data = {}
  
  for item in list_data:
    # If this is a single key field, then 
    if type(key_field) != list:
      data[item[key_field]] = item
      
    else:
      key = multi_key_separator.join(key_field)
      data[key] = item
  
  return data


def ConvertListToDictList(list_data, key_field):
  """Returns a dict of lists of dicts keyed on key_field of list_data item dict data.  list_data is a list of dicts.
  
  key_field can be string (single value) or list (combined value into a key.field.value dict key)
  """
  data = {}
  
  for item in list_data:
    # If this is a single key field, then 
    if type(key_field) != list:
      if item[key_field] not in data:
        data[item[key_field]] = []
      
      data[item[key_field]].append(item)
      
    else:
      key = multi_key_separator.join(key_field)
      if key not in data:
        data[key] = []
      
      data[key].append(item)
  
  return data


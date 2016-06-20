"""
Datasource: MySQL: Querying
"""

import mysql.connector


class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None


def Query(sql):
	conn = mysql.connector.connect(user='root', password='root', host='opsdb', database='opsdb')
	cursor = conn.cursor(cursor_class=MySQLCursorDict)
	
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


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


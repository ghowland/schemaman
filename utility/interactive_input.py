"""
Interactive Input
"""

import readline


# Set up readline globally, so we dont need an init function for this to work
readline.parse_and_bind('tab: complete')
readline.parse_and_bind('set editing-mode vi')


def ReadLine(prompt):
  """Read a line, using readline controls."""
  line = raw_input(prompt)
  
  return line



def ReplaceUncleanChars(text, replacement_char='_'):
  unclean_chars = ' \t:;\'",.<>/?=+[]{}-`~!@#$%^&*()'
  
  for char in unclean_chars:
    text = text.replace(char, replacement_char)
  
  return text


def GetInputField(data, key, label, short_label, intro_string, force_lower=False, force_clean=False, force_strip=False, force_int=False, options=None):
  done = False

  while not done:
    # New input
    if key not in data:
      print '\n%s' % intro_string
      
      input_text = ReadLine('\nEnter %s: ' % short_label)
    
    # Else, we have existing input, prompting to continue or change
    else:
      print 'Hit enter (blank entry) if this is OK, or enter a new %s' % label
      print '\n%s: %s' % (label, data[key])
      input_text = ReadLine('\nEnter an updated %s, or enter to accept: ' % short_label)
    
    
    # If we got data, ingest it
    if input_text.strip():
      data[key] = input_text
      
      # Lower case
      if force_lower:
        data[key] = data[key].lower()
      
      # Strip
      if force_strip:
        data[key] = data[key].strip()
      
      # Clean characters
      if force_clean:
        data[key] = ReplaceUncleanChars(data[key])
      
      # Integer
      if force_int:
        try:
          data[key] = int(data[key])
        except ValueError, e:
          print '\nYou entered an invalid integer: %s\n' % data[key]
          
          del data[key]
      
      # Options
      if options and data[key] not in options:
        print '\nYou did not enter any of the possible options.\n'
        
        del data[key]
      
      # Alert them if we have changed their input
      if key in data and input_text != data[key]:
        print '\nThe %s has been re-written as this: %s\n' % (label, data[key])
    
    # Else, we have the data, and got empty input (newline = continue)
    elif key in data:
      # If we 
      if options and data[key] not in options:
        print 'The value must be one of these valid options: %s' % ', '.join(options)
      
      else:
        done = True


def CollectInitializationDataFromInput():
  """Collect information about this schema from the user interactively via stdin."""
  data = {}
  
  # Get the Schema Alias
  intro_string = 'Schema Names can be anything, and should be considered the "Fancy Name" with spaces and capitals, etc.\nSchema Alias should not have any spaces or capital letters, as they will be used as YAML file names, and in other places as variables.'
  GetInputField(data, 'alias', 'Schema Alias', 'alias', intro_string, force_lower=True, force_strip=True, force_clean=True)
  
  
  # Get the Schema Name
  intro_string = 'Schema Name should not have any spaces or capital letters, as they will be used as YAML file names, and in other places as variables.'
  GetInputField(data, 'name', 'Schema Name', 'name', intro_string, force_strip=True)
  
  
  # Get the Owner User
  intro_string = 'Owner User should be a username, or email, or whatever you use for authentication.  Leading and trailing white space is stripped.'
  GetInputField(data, 'owner_user', 'Owner User', 'user', intro_string, force_strip=True)
  
  
  # Get the Owner Group
  intro_string = 'Owner Group should be a username, or email, or whatever you use for authentication.  Leading and trailing white space is stripped.'
  GetInputField(data, 'owner_group', 'Owner Group', 'group', intro_string, force_strip=True)
  
  
  # Get the DB Host
  intro_string = 'Database Host is a valid hostname for a host that exists now and has a database on it.'
  GetInputField(data, 'database_host', 'Database Host', 'host', intro_string, force_strip=True)
  
  
  # Get the DB Port
  intro_string = 'Database Port is an integer which the database is running on.  Use 0 if there is no port.'
  GetInputField(data, 'database_port', 'Database Port', 'port', intro_string, force_strip=True, force_int=True)
  
  
  # Get the DB Name
  intro_string = 'Database name is the name of the database in the backend database.'
  GetInputField(data, 'database_name', 'Database Name', 'database name', intro_string, force_strip=True)
  
  
  # Get the DB User
  intro_string = 'Database user is a valid username to authenticate against the specified database.'
  GetInputField(data, 'database_user', 'Database User', 'user', intro_string, force_strip=True)
  
  
  # Get the DB Password
  intro_string = 'A path to a secret database password, stored in a text file on your local file system.'
  GetInputField(data, 'database_password_path', 'Database Password Path', 'password path', intro_string, force_strip=True)
  
  
  # Get the DB Type
  #TODO(g): Get these from our possible DB elements
  options = ['mysql_56']
  intro_string = 'Select the type of database backend from the list of options: %s' % ', '.join(options)
  GetInputField(data, 'database_type', 'Database Type', 'type', intro_string, force_strip=True, options=options)
  
  
  return data

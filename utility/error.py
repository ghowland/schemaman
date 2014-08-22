"""
Error Reporting
"""


import sys


def Error(text, options, exit_code=1):
  """Fail with an error. Options are required to deliver proper output."""
  #output = {'error': text, 'exit_code':exit_code}
  #
  ## Format errors and output them, in the specified fashion
  #format.FormatAndOuput(output, options)

  sys.stderr.write('ERROR: %s\n' % text)
  
  #TODO(g): Clean-up work we have done so far, so this system is not left in
  # an unusable state
  pass#...
  
  # Exit
  sys.exit(exit_code)


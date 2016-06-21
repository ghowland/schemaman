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


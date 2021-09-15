from ..utils import trace_unhandled_exceptions

@trace_unhandled_exceptions
def migrate_attachment(path):
  print('hello')
  # discord snowflakes are almost always 64-bit in binary
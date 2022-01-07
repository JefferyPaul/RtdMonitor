import logging
from .simpleLogger import MyLogger

my_logger = MyLogger(name='RtdMonitor', level=logging.INFO, is_file=True,)

import matplotlib
matplotlib.use('Agg')
import logging

# Configure logging
name = 'daq'
fmt = '%(asctime)s\t%(name)-20s%(levelname)s\t%(message)s'
datefmt = '%m/%d/%Y %I:%M:%S %p'
formatter = logging.Formatter(fmt,datefmt)
logging.basicConfig(filename='{0}.log'.format(name), filemode='w', level=10, format=fmt, datefmt=datefmt)
logger = logging.getLogger(name)
# Add console output
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)
logger.addHandler(stream_handler)
# Add error log
err_handler = logging.FileHandler('{0}.err'.format(name), mode='w')
err_handler.setFormatter(formatter)
err_handler.setLevel(logging.WARNING)
logger.addHandler(err_handler)
# Supress other logging
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('suds').setLevel(logging.CRITICAL)

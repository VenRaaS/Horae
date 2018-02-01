import sys
import logging


#-- logging setup
#   see https://docs.python.org/2/howto/logging.html#configuring-logging
formatter = logging.Formatter("[%(asctime)s][%(levelname)s] %(filename)s(%(lineno)s) %(name)s - %(message)s", datefmt='%Y-%m-%d %H:%M:%S')

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

#fh = logging.FileHandler("{0}.log".format(__name__))
#fh.setLevel(logging.DEBUG)
#fh.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(ch)
#logger.addHandler(fh)

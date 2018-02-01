import sys
import csv

from logger import logger


def has_header(fn):
    try:
        with open(fn, 'rb') as f:
            reader = csv.reader(f, delimiter='\t')
            row1 = next(reader)
            return not any(c.isdigit() for c in row1)
    except Exception as e:
        logger.error(e, exc_info=True)


if __name__ == '__main__':
   print has_header(sys.argv[1])
        

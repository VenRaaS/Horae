import sys
import csv
import subprocess
from logger import logger


def has_header(ffn):
    with open(ffn, 'rb') as f:
        reader = csv.reader(f, delimiter='\t')
        row1 = next(reader)
        return not any(c.isdigit() for c in row1)

def remove_dq2space(ffn):
    cmd = "sed -i 's/\"/ /g' {}".format(ffn)
    logger.info(cmd)
    subprocess.call([cmd], shell=True)

def remove_zero_datetime(ffn):
    cmd = "sed -i 's/0000-00-00[^\t]*//g' {}".format(ffn)
    logger.info(cmd)
    subprocess.call([cmd], shell=True)

def replace_c_cs(ffn):
    cmd = r"sed -r -i 's/([0-9]+),([0-9]+)/\1, \2/g' {}".format(ffn)
    logger.info(cmd)
    subprocess.call([cmd], shell=True)

def lowercase_firstLine(ffn):
    cmd = r"sed -ri '1s/\w+/\L&/g' {}".format(ffn)
    logger.info(cmd)
    subprocess.call([cmd], shell=True)


if __name__ == '__main__':
   print has_header(sys.argv[1])
        

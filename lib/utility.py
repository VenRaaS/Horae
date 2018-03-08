import os
import sys
import csv
import subprocess
import json
import time
import logging

import requests


logger = logging.getLogger(__file__)



def basename(ffn):
	fn = os.path.basename(ffn)
	bn = os.path.splitext(fn)[0]
	return bn

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

def returnOnlyIfCountStable_es(url, chk_interval_sec=30):
    r = requests.get(url)
    cnt1 = json.loads(r.text)['count']
    while True:
        time.sleep(chk_interval_sec)

        r = requests.get(url)
        cnt2 = json.loads(r.text)['count']
        d = cnt1 - cnt2
        if 0 == d:
            logger.info('return due to count is stable, {} -> {} = {} in ES {}'.format(cnt1, cnt2, d, url))
            break;
        
        logger.info('continue due to, {} -> {} = {} in ES {}'.format(cnt1, cnt2, d, url))
        cnt1 = cnt2
  

if __name__ == '__main__':
   print has_header(sys.argv[1])
        

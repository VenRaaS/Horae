from enum import Enum

#-- Enum - aenum 
#   see https://bitbucket.org/stoneleaf/aenum
#   see https://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python?page=1&tab=votes#tab-top

#-- each Topic is seprated by a SPACE
EnumTopic = Enum('EnumTopic', 'bucket_ven-custs bigquery es_cluster')

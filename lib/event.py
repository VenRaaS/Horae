from enum import Enum

#-- Enum - aenum 
#   see https://bitbucket.org/stoneleaf/aenum
#   see https://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python?page=1&tab=votes#tab-top
class EnumEvent(Enum) :
    DEFAULT = 1
    OBJECT_FINALIZE = 2
    OBJECT_DELETE = 3
    CRON_SCHEDULER = 4



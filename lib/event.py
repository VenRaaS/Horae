from enum import Enum

#-- Enum - https://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python?page=1&tab=votes#tab-top
class EnumEvent(Enum) :
    OBJECT_FINALIZE = 1
    OBJECT_DELETE = 2

EnumTopic = Enum('EnumTopic', 'bucket_ven-custs bigquery_unima_gocc')



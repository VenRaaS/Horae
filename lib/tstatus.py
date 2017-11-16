import threading
import datetime
from enum import Enum


class EnumState(Enum) :
    NONE = 0
    INIT = 1
    RUNNING = 2
    PUB = 3
    END = 4


class TaskStatus :
    def __init__(self) :
        self.rlock = threading.RLock()
        self.state = EnumState.NONE
        self.start_dt = None
        self.end_dt = None
    
    def init(self) :
        with self.rlock:
            self.state = EnumState.INIT
            self.start_dt = None
            self.end_dt = None

    def start(self) :
        with self.rlock: 
            self.start_dt = datetime.datetime.now()
            self.state = EnumState.RUNNING

    def pub(self) :
        with self.rlock: 
            self.state = EnumState.PUB

    def end(self) :
        with self.rlock: 
            self.state = EnumState.END
            self.end_dt = datetime.datetime.now()

    def elapsed_sec(self) :
        with self.rlock: 
            delta_t = abs(self.end_dt - self.start_dt) if EnumState.END == self.state else abs(datetime.datetime.now() - self.start_dt)
            return delta_t.seconds

    def elapsed_afterend_sec(self):
        with self.rlock: 
            delta_t = abs(datetime.datetime.now() - self.end_dt) if EnumState.END == self.state else 0
            return delta_t.seconds

    def __str__(self) :
       return str(self.__dict__)


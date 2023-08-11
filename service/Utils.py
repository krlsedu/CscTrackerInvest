import os
from datetime import datetime

from service.Interceptor import Interceptor


class Utils(Interceptor):
    def __init__(self):
        super().__init__()

    def work_time(self):
        n = datetime.now()
        t = n.timetuple()
        y, m, d, h, min, sec, wd, yd, i = t
        h = h - 3
        return 8 <= h <= 19

    def work_day(self):
        n = datetime.now()
        t = n.timetuple()
        y, m, d, h, min, sec, wd, yd, i = t
        return wd < 5

    def read_file(self, file_name):
        file_dir = os.path.dirname(os.path.realpath('__file__'))
        file_name = os.path.join(file_dir, file_name)
        file_handle = open(file_name)
        content = file_handle.read()
        file_handle.close()
        return content

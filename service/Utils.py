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
        return 10 <= h <= 18

    def work_day(self):
        n = datetime.now()
        t = n.timetuple()
        y, m, d, h, min, sec, wd, yd, i = t
        return wd < 5

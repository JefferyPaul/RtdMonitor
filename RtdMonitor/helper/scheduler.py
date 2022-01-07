import datetime
import time
import abc
import threading

from . import my_logger


class ScheduleRunner:
    """
    self.star_loop() 使用多线程，因为
        调用 self._start 和 self._end 是可能出现阻塞的
    """
    def __init__(self,
                 running_time=[[datetime.time(0, 0, 0), datetime.time(23, 59, 59)], ],
                 loop_interval=60 * 1):
        self._schedule_running_time = running_time
        self._schedule_in_running = False
        self._schedule_loop_interval = loop_interval

    @abc.abstractmethod
    def _start(self):
        pass

    @abc.abstractmethod
    def _end(self):
        pass

    def start_loop(self):
        print('启动运行...')
        print('等待进入运行时间区间')
        # 初始化，用于检查上一次上传的时间，防止长时间没有上传
        while True:
            time_now = datetime.datetime.now().time()
            is_in_running_time = True in [
                (time_now >= time_range[0]) and (time_now <= time_range[1])
                for time_range in self._schedule_running_time
            ]

            # 运行时间中
            if self._schedule_in_running and is_in_running_time:
                pass
            # 不在运行时间
            elif (not self._schedule_in_running) and (not is_in_running_time):
                pass
            # 开始
            elif (not self._schedule_in_running) and is_in_running_time:
                self._schedule_in_running = True
                my_logger.info('开始运行...')
                self._start()
            # 结束运行
            else:
                self._schedule_in_running = False
                my_logger.info('暂停运行...')
                self._end()

            #
            time.sleep(self._schedule_loop_interval)

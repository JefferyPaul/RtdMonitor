import os
from datetime import datetime, time, date
import sys
import json

PATH_FILE = os.path.abspath(__file__)
PATH_PROJECT = os.path.abspath(os.path.join(PATH_FILE, '../..'))
PATH_CONFIG = os.path.join(PATH_PROJECT, 'Config', 'Config.json')
sys.path.append(PATH_PROJECT)

from RtdMonitor.monitor import RtdMonitorEngine


if __name__ == '__main__':
    d_config = json.loads(open(PATH_CONFIG, encoding='utf-8').read())
    running_time = []
    for _time in d_config['running_time']:
        running_time.append([
            datetime.strptime(_time[0], '%H%M%S').time(),
            datetime.strptime(_time[1], '%H%M%S').time(),
        ])

    engine = RtdMonitorEngine(
        data_path=d_config['data_path'],
        checking_interval=d_config['checking_interval'],
        nrows=d_config['rows'], ncols=d_config['cols'],
        running_time=running_time
    )
    engine.start_loop()

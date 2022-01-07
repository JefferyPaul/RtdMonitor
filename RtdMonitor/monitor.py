import os
import shutil
import sys
from datetime import datetime, date, time
from time import sleep
import threading
import logging
from collections import defaultdict
from typing import List, Dict

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
import matplotlib.dates as mdate

from RtdMonitor.helper.scheduler import ScheduleRunner
from RtdMonitor.helper.simpleLogger import MyLogger


class RtdPlotter:
    """
    1，持续接收画图信息，
    2，实现画图功能，
    3，画图并展示，   plt.show()
    1，持续接收画图信息，并在信息变更后立即重新再画。   plt.draw()
    """
    def __init__(
            self, engine,
            nrows, ncols, title
    ):
        self.engine = engine

        # 创建子图
        self.fig, self.axs = plt.subplots(
            nrows, ncols,
            figsize=self._cal_fig_size(nrows, ncols)
        )
        self.fig.subplots_adjust(
            left=0.05, right=0.98,
            bottom=0.05, top=0.96,
            wspace=0.18, hspace=0.3
        )  # 设置子图之间的间距
        self.fig.canvas.set_window_title(title)  # 设置窗口标题

        # 子图字典，key为子图的序号，value为子图句柄
        self._ax_list: List[Axes] = []
        for i in range(nrows):
            for j in range(ncols):
                self._ax_list.append(self.axs[i, j])

    @staticmethod
    def _cal_fig_size(nrows, ncols):
        row_size = 3 * nrows
        col_size = 2 * ncols
        return row_size, col_size

    @staticmethod
    def show():
        """ 显示曲线 """
        plt.show()

    def update_plot(self, index, x, y, symbol):
        """
        更新指定序号的子图
        :param index: 子图序号
        :param x: 横轴数据
        :param y: 纵轴数据
        :return:
        """
        # X轴数据必须和Y轴数据长度一致
        if len(x) != len(y):
            ex = ValueError("x and y must have same first dimension")
            raise ex

        self._ax_list[index].clear()  # 清空子图数据

        self._ax_list[index].step(x, y, where="post")  # 绘制最新的数据
        self._ax_list[index].set_title(symbol, fontsize=8)
        self._ax_list[index].tick_params(
            labelsize=8,
            # labelrotation=15
        )
        self._ax_list[index].xaxis.set_major_formatter(mdate.DateFormatter('%H:%M'))

        plt.draw()


class RtdMonitorEngine(ScheduleRunner):
    """
    1，时间控制；[[time(0, 0, 0), time(23, 59, 59)], ]
    2，数据读取并处理
    3，判断需要画什么
    4，将画图信息传送到 RtdPlotter


    data_path 结构要求：
    ./data_path/
        -nameA
        -nameB
            -{time}.csv
            -{time}.csv
    选取[nameA, nameB中名字最大的文件夹]，当最大文件夹更换后，数据会重新初始化

    数据缓存：
    {
        nameA: [{
            'dt': datetime.strptime(_dt, self._dt_pattern),
            'tp': float(_tp)
        }, ],
    }
    """
    def __init__(
            self,
            data_path, checking_interval, nrows, ncols,
            running_time: list,     # ScheduleRunner的参数
            logger=MyLogger('RtdMonitor'),
            dt_pattern='%Y%m%d %H%M%S',
    ):
        super(RtdMonitorEngine, self).__init__(running_time=running_time, loop_interval=60)

        self._data_path = os.path.abspath(data_path)
        self._checking_interval = int(checking_interval)
        self._nrows = nrows
        self._ncols = ncols
        self._plot_count = nrows * ncols
        self.logger: logging.Logger = logger
        self._dt_pattern = dt_pattern

        assert os.path.isdir(self._data_path)

        self._looping = False
        self._my_thread: None or threading.Thread = None
        self._last_folder_path = ''
        self._last_file = ''          # 记录最新读取的文件
        self.data = defaultdict(list)
        self._last_data = defaultdict(dict)
        self._data_changed_symbols = []     # 有最新数据更新的symbol
        self._plotting_symbols = []         # 正在（上一次）展示的是哪一些

        self.plotter = RtdPlotter(
            engine=self, nrows=nrows, ncols=ncols,
            title='Real Time Signal'
        )

    def _start(self):
        """
        :return:
        """
        self._looping = True
        self._my_thread = threading.Thread(target=self._running_loop)
        self._my_thread.start()
        self.plotter.show()     # 首次启动画图

    def _running_loop(self):
        self.logger.info('start looping')
        while self._looping:
            # 数据读取
            has_new_file: bool = self._read_files_data()
            # 数据处理，转换成画图的数据
            if has_new_file:
                self._data_to_plot()
            # 间隔
            sleep(self._checking_interval)

    def _end(self):
        # 阻塞,确保仅有一个 线程 在运行
        # 直接用 ScheduleRunner._schedule_in_running 来判断 和 控制，不需要另外 结束
        self.logger.info('正在等待线程结束...')
        self._looping = False
        if self._my_thread:
            self._my_thread.join()
        self.logger.info('线程已终止!')

    def _refresh_data(self):
        self.data = defaultdict(list)
        self._last_file = ''
        self._last_data = defaultdict(dict)
        self._data_changed_symbols = []

    def _read_files_data(self) -> bool:
        """
        检查 读取文件数据,
        缓存在 self.data 中
        """
        def _read_data(p):
            with open(p) as f:
                l_lines = f.readlines()
            for line in l_lines:
                line = line.strip()
                if line == '':
                    continue
                _dt, _symbol, _tp = line.split(',')
                dt = datetime.strptime(_dt, self._dt_pattern)
                tp = float(_tp)
                _new_data = {'dt': dt, 'tp': tp}
                if not self._last_data.get(_symbol):
                    # 没有 last_data,  初次读取
                    self.data[_symbol].append(_new_data.copy())
                    if _symbol not in self._data_changed_symbols:
                        self._data_changed_symbols.append(_symbol)
                    self._last_data[_symbol] = _new_data.copy()
                else:
                    # 只处理与之前的数据 不同的数据
                    if float(_tp) != self._last_data[_symbol]['tp']:
                        self.data[_symbol].append(_new_data.copy())
                        if _symbol not in self._data_changed_symbols:
                            self._data_changed_symbols.append(_symbol)
                        if dt > self._last_data[_symbol]['dt']:
                            self._last_data[_symbol] = _new_data.copy()

        # 查找最新的文件夹
        newest_folder = max([
            i for i in os.listdir(self._data_path)
            if os.path.isdir(os.path.join(self._data_path, i))
        ])
        if newest_folder != os.path.basename(self._last_folder_path):
            self.logger.info('find new folder, refresh cache data')
            self._refresh_data()
            self._last_folder_path = os.path.join(self._data_path, newest_folder)

        # 只读取新的文件
        l_file_names = sorted([i for i in os.listdir(self._last_folder_path) if i > self._last_file])
        if l_file_names:
            self._last_file = max(l_file_names)
            self.logger.info(f'reading files data, newest files: {self._last_file}')
            for file_name in l_file_names:
                path_file = os.path.join(self._last_folder_path, file_name)
                try:
                    _read_data(path_file)
                except Exception as e:
                    self.logger.error(f'reading file error, {path_file}, {e}')
            return True
        else:
            self.logger.info('no new data file')
            return False

    def _data_to_plot(self):
        """
        """

        if self._data_changed_symbols:
            self.logger.info(f'data changed symbols: {",".join(self._data_changed_symbols)}')
        else:
            self.logger.info('no data changed')
            return

        # 选择需要展示的symbol
        if len(self.data) > self._plot_count:
            # 数据 比 窗口多，需要优先展示发生变化的数据
            if len(self._data_changed_symbols) >= self._plot_count:
                self._plotting_symbols = sorted(self._data_changed_symbols, key=lambda x: x.lower())[:self._plot_count]
            else:
                _ = self._plotting_symbols.copy()
                self._plotting_symbols = self._data_changed_symbols.copy()
                for i in _:
                    if i not in self._plotting_symbols:
                        self._plotting_symbols.append(i)
                        if len(self._plotting_symbols) == self._plot_count:
                            break
                if len(self._plotting_symbols) < self._plot_count:
                    for i in sorted(self.data.keys(), key=lambda x: x.lower()):
                        if i not in self._plotting_symbols:
                            self._plotting_symbols.append(i)
                            if len(self._plotting_symbols) == self._plot_count:
                                break
        else:
            self._plotting_symbols = list(self.data.keys())
        self._plotting_symbols.sort(key=lambda x: x.lower())

        # 更新画图信息
        self.logger.info('data to plotter')
        for n, symbol in enumerate(self._plotting_symbols):
            self._plot(index=n, data=self.data[symbol], symbol=symbol)
        self._data_changed_symbols.clear()

    def _plot(self, index, data: List[dict], symbol):
        self.plotter.update_plot(
            index=index,
            x=[i['dt'] for i in data],
            y=[i['tp'] for i in data],
            symbol=symbol
        )

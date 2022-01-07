import subprocess
import datetime
import os
from enum import Enum
from typing import Tuple
import logging

from .constant import MESSAGE_CLIENT_ADDRESS


class RunException(Enum):
    """
    """
    TimeOut = 0
    Error = -1
    Success = 1


def _run_mc(s_cmd, timeout, logger: logging.Logger) -> Tuple[RunException, str] or None:
    p = subprocess.Popen(
        s_cmd,
        cwd=MESSAGE_CLIENT_ADDRESS,
        stdout=subprocess.PIPE,
        shell=True,
    )

    try:
        outs, errs = p.communicate(timeout=timeout)
        output_s = str(outs, encoding="utf-8")
    except subprocess.TimeoutExpired as e:
        p.kill()
        logger.error('调用 MessageClient 超时:')
        logger.error(e)
        return RunException.TimeOut, e
        # outs, errs = p.commnuicate()
    except Exception as e:
        p.kill()
        logger.error('调用 MessageClient 失败:')
        logger.error(e)
        return RunException.Error, e
    else:
        if 'Exception' in output_s:
            p.kill()
            logger.warning('调用 MessageClient 失败:')
            logger.warning(output_s)
            return RunException.TimeOut, output_s
        else:
            p.kill()
            return None, output_s.strip()


# 上传
def send_file(mc_ip, mc_port, upload_name, path_target_file, timeout, logger: logging.Logger,
              max_try: int = 5) -> datetime.datetime or None:
    """
    :param mc_ip:
    :param mc_port:
    :param upload_name:
    :param path_target_file:
    :param timeout:
    :param logger:
    :param max_try:
    :return: None 代表运行失败
    """
    assert type(max_try) == int

    if max_try < 1:
        max_try = 1

    for n in range(max_try):
        s_cmd = '''TradingPlatform.MessageClient.exe %s %s sendfile "%s" "%s" ''' % (
            mc_ip, mc_port, upload_name, path_target_file)

        error_type, msg = _run_mc(s_cmd=s_cmd, timeout=timeout, logger=logger)
        if not error_type:
            return datetime.datetime.now()
        else:
            if error_type == RunException.TimeOut:
                logger.error(f'第{n+1}次运行，超时')
                continue
            if error_type == RunException.Error:
                logger.error(f'第{n + 1}次运行，失败')
                continue

    logger.error('超过最大运行次数')
    return None


def get_file(mc_ip, mc_port, file_key: str, output_root, timeout, logger: logging.Logger,
              max_try: int = 5) -> datetime.datetime or None:
    """
    :param mc_ip:
    :param mc_port:
    :param file_key:
    :param output_root:
    :param timeout:
    :param logger:
    :param max_try:
    :return: None 代表运行失败
    """
    assert type(max_try) == int

    if not os.path.exists(output_root):
        os.mkdir(output_root)

    if max_try < 1:
        max_try = 1

    path_file_output = os.path.join(output_root, file_key)
    for n in range(max_try):
        s_cmd = '''TradingPlatform.MessageClient.exe %s %s getfile "%s" "%s" ''' % (
            mc_ip, mc_port, file_key, path_file_output)

        error_type, return_msg = _run_mc(s_cmd=s_cmd, timeout=timeout, logger=logger)
        if not error_type:
            # logger.info(f'downloaded {file_key}')
            return datetime.datetime.now()
        else:
            # exception_type, exception_string = error_msg
            if error_type == RunException.TimeOut:
                logger.error(f'第{n + 1}次运行，超时')
                continue
            if error_type == RunException.Error:
                logger.error(f'第{n + 1}次运行，失败')
                continue
    logger.error(f'超过最大运行次数, download: {file_key}')
    return None


def get_message(mc_ip, mc_port, message_key: str, timeout, logger: logging.Logger,
              max_try: int = 5) -> str or None:
    """
    :param mc_ip:
    :param mc_port:
    :param file_key:
    :param timeout:
    :param logger:
    :param max_try:
    :return: None 代表运行失败
    """
    assert type(max_try) == int

    if max_try < 1:
        max_try = 1

    for n in range(max_try):
        s_cmd = '''TradingPlatform.MessageClient.exe %s %s getmessage "%s"''' % (
            mc_ip, mc_port, message_key)
        # print(s_cmd)

        error_type, msg = _run_mc(s_cmd=s_cmd, timeout=timeout, logger=logger)
        # print(msg)
        if error_type:
            logger.error(msg)
            continue
        if msg:
            return msg.split('<<')[1]
    logger.error(f'超过最大运行次数, download: {message_key}')
    return None


# 上传
def send_message(mc_ip, mc_port, key, msg, timeout, logger: logging.Logger,
              max_try: int = 5) -> datetime.datetime or None:
    """
    :param mc_ip:
    :param mc_port:
    :param upload_name:
    :param timeout:
    :param logger:
    :param max_try:
    :return: None 代表运行失败
    """
    assert type(max_try) == int

    if max_try < 1:
        max_try = 1

    for n in range(max_try):
        s_cmd = '''TradingPlatform.MessageClient.exe %s %s sendmessage "%s" "%s"''' % (
            mc_ip, mc_port, key, msg)
        error_type, msg = _run_mc(s_cmd=s_cmd, timeout=timeout, logger=logger)
        if not error_type:
            return datetime.datetime.now()
        else:
            # exception_type, exception_string = error_msg
            if error_type == RunException.TimeOut:
                logger.error(f'第{n+1}次运行，超时')
                continue
            if error_type == RunException.Error:
                logger.error(f'第{n + 1}次运行，失败')
                continue
    logger.error('超过最大运行次数')
    return None


def status():
    pass

import datetime
import os
import logging
from collections import namedtuple
import subprocess

P_PKG = os.path.dirname(os.path.abspath(__file__))
WARNING_BOARD_ADDRESS = os.path.join(P_PKG, 'TradingPlatform.WarningBoard')


def run_warning_board(warning_msg: str, timeout_continue=0.1):
    assert type(warning_msg) is str
    s_cmd = '%s "%s"' % ('TradingPlatform.WarningBoard.exe', warning_msg)
    p = subprocess.Popen(
        s_cmd,
        cwd=WARNING_BOARD_ADDRESS,
        stdout=subprocess.PIPE,
        shell=True,
    )

    try:
        outs, errs = p.communicate(timeout=timeout_continue)
        output_s = str(outs, encoding="utf-8")
    except:
        return None
    else:
        print(output_s)
        return None
    finally:
        p.kill()

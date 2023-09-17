# -*- coding: utf-8 -*-
# @Time : 2023/9/4/001 21:42
# @Author : 不归
# @FileName: constants.py

import os
import sys
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from loguru import logger as log

DEBUG = True
BASE_PATH = Path(__file__).parent.parent
dt_format: str = "%Y%m%d"


def set_glob(key, value):
    globals()[key] = value


def get_glob(key, default=None):
    try:
        return globals()[key]
    except KeyError:
        print(f"未找到全局参数{key}")
        return default


def init_log():
    log.remove(handler_id=None)

    error_log_file_path = get_glob("BASE_PATH") / 'logs/error_{time:YYYY-MM-DD}.log'
    warning_log_file_path = get_glob("BASE_PATH") / 'logs/warning_{time:YYYY-MM-DD}.log'
    success_log_file_path = get_glob("BASE_PATH") / 'logs/success_{time:YYYY-MM-DD}.log'
    info_log_file_path = get_glob("BASE_PATH") / 'logs/info_{time:YYYY-MM-DD}.log'
    debug_log_file_path = get_glob("BASE_PATH") / 'logs/debug_{time:YYYY-MM-DD}.log'

    # 错误日志
    err_log = log.add(
        error_log_file_path,
        # format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        filter=lambda x: True if x["level"].name == "ERROR" else False,
        rotation="00:00", retention=7, level='ERROR', encoding='utf-8'
    )
    # WARNING日志
    war_log = log.add(
        warning_log_file_path,
        # format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        filter=lambda x: True if x["level"].name == "WARNING" else False,
        rotation="00:00", retention=7, level='WARNING', encoding='utf-8',
    )
    # SUCCESS日志
    suc_log = log.add(
        success_log_file_path,
        # format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        filter=lambda x: True if x["level"].name == "SUCCESS" else False,
        rotation="00:00", retention=7, level='SUCCESS', encoding='utf-8',
    )
    # INFO日志
    info_log = log.add(
        info_log_file_path,
        # format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        filter=lambda x: True if x["level"].name == "INFO" else False,
        rotation="00:00", retention=7, level='INFO', encoding='utf-8',
    )
    # DEBUG日志
    debug_log = log.add(
        debug_log_file_path,
        # format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
        rotation="00:00", retention=7, level='DEBUG', encoding='utf-8'
    )

    if not get_glob("DEBUG"):
        log.add(sys.stderr, level="INFO")
    else:
        log.add(sys.stderr, level="DEBUG")

    log.debug(f"当前项目根目录为：{get_glob('BASE_PATH')}")


def init_api_param():
    if getattr(init_api_param, 'has_run', False):
        return
    init_api_param.has_run = True

    env_path = get_glob("BASE_PATH") / 'conf/api.env'
    log.debug(f"准备加载API配置文件：{env_path}")
    load_dotenv(find_dotenv(env_path))
    log.debug(f"当前API地址为：{os.environ.get('api_url')}")


def init_feishu_param():
    if getattr(init_feishu_param, 'has_run', False):
        return
    init_feishu_param.has_run = True

    env_path = get_glob("BASE_PATH") / 'conf/feishu.env'
    log.debug(f"准备加载Feishu配置文件：{env_path}")
    load_dotenv(find_dotenv(env_path))


def init_db_param():
    if getattr(init_db_param, 'has_run', False):
        return
    init_db_param.has_run = True

    if not get_glob("DEBUG"):
        cfg = 'prod.env'
        log.disable("conf.constants")
    else:
        cfg = 'dev.env'
    # 加载.env文件到环境变量
    env_path = get_glob("BASE_PATH") / 'conf' / cfg
    log.debug(f"准备加载数据库配置文件：{env_path}")
    load_dotenv(find_dotenv(env_path))
    log.debug(f"当前数据库地址为：{os.getenv('db_host')}")


def init():
    """
    用于初始化全局常量及log对象
    :return:
    """
    set_glob("BASE_PATH", BASE_PATH)
    set_glob("DEBUG", DEBUG)

    init_log()

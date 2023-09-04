# -*- coding: utf-8 -*-
# @Time : 2023/3/13/013 21:42
# @Author : 不归
# @FileName: config.py
"""
精简版配置文件，默认存储于%userprofile%/.czsc路径
"""

from orjson import orjson

from conf.constants import *
from libs.tools import check_path_exist

home = Path.home()

if not DEBUG:
    log.disable('libs.cache')


class Cache:
    def __init__(self, name="", filepath=None):
        self.cfg_path = filepath
        if not filepath:
            mk = home / ".czsc/"
            check_path_exist(mk)
            self.cfg_path = (mk / "config") if not name else (mk / f"{name}")

    def get(self):
        path = Path(str(self.cfg_path) + '.json')
        log.debug(f"正在读取缓存记录，地址：{self.cfg_path}")
        ls: list = []
        if Path.exists(path):
            with open(path, "rb") as f:
                ls = orjson.loads(f.read())
        else:
            log.debug(f"加载缓存错误，未找到文件：{path}")
        return ls

    def set(self, values: list):
        path = Path(str(self.cfg_path) + '.json')
        check_path_exist(self.cfg_path.parent)
        with open(path, "wb") as f:
            f.write(orjson.dumps(values))
        log.debug(f"已写入缓存文件：{path}")

    def remove(self):
        path = Path(str(self.cfg_path) + '.json')
        log.debug(f"准备删除缓存文件：{path}")
        Path.unlink(path) if Path.exists(path) else None

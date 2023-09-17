# -*- coding: utf-8 -*-
# @Time : 2023/3/13/013 21:42
# @Author : 不归
# @FileName: cache.py
"""
持久化缓存对象，默认存储于 %userprofile%/.czsc/ 路径
"""

from orjson import orjson

from conf.constants import *
from libs.tools import check_path_exist

home = Path.home()


class Cache:
    def __init__(self, name: str = "", filepath: str | Path | None = None):
        self.path = Path(str(filepath) + '.json')
        if not filepath:
            mk = home / ".czsc/"
            check_path_exist(mk)
            self.path = (mk / "config.json") if not name else (mk / f"{name}.json")

    def __str__(self) -> str:
        p = self.path
        return p.stem

    def get(self):
        log.debug(f"正在读取缓存记录，地址：{self.path}")
        ls: list = []
        if Path.exists(self.path):
            with open(self.path, "rb") as f:
                ls = orjson.loads(f.read())
        else:
            log.debug(f"加载缓存错误，未找到文件：{self.path}")
        return ls

    def set(self, values: list):
        check_path_exist(self.path.parent)
        with open(self.path, "wb") as f:
            f.write(orjson.dumps(values))
        log.debug(f"已写入缓存文件：{self.path}")

    def remove(self):
        log.debug(f"准备删除缓存文件：{self.path}")
        Path.unlink(self.path) if Path.exists(self.path) else None

# qds - 量化数据服务

* **全称： Quantitative data service**
* 旨在帮助各位道友简化搭建本地数据仓的第一步工作
* 基于 clickhouse数据库

## 项目贡献

* 目前实现A股`股票代码、日K、1分钟K、复权因子`的收集入库。
* 本项目支持多数据源，目前完成`TS(tushare)`源；`GM、QMT`暂未完成更新，有急需请先自行修改。
* 支持中断后续传、更新。

## 安装使用

**注意:** python 版本必须大于等于 3.10 （更低版本未测试）

1. 使用前请自行安装 clickhouse
2. `git git@github.com:hanzch/qds.git`
3. 重命名 `conf/dev.env.bak` 去掉.bak，按需修改内容
4. `script/1.init_db.py` 运行后完成建库及测试数据写入*(如不需要请自行注释对应代码)*
5. 执行`jobs.py`开始下载，目前依赖于`czsc.fsa`模块，如不需要请自行注释或删除

## 使用前必看

* 这是个人开发的项目，可能存在BUG，如有问题请联系；
* 目前开发完成度不高，暂时不准备写文档，没有能力看懂源码的，请谨慎使用。
* 如果你发现了项目中的 Bug，可以先读一下《[如何有效地报告 Bug](https://www.chiark.greenend.org.uk/~sgtatham/bugs-cn.html)
  》，然后在 [issues](https://github.com/hanzch/qds/issues) 中报告 Bug
* **免责声明：项目开源仅用于技术交流！**

## 关联库

* [czsc - 缠中说禅技术分析工具](https://github.com/waditu/czsc/)

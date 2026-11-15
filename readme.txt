秋窗客流分析账本
运行环境：Python 3.11，已按 requirements.txt 预装依赖。
启动：python -m autumn_ledger serve --database autumn-ledger.sqlite3
接口说明：服务启动后访问 /docs。
异步分析作业：POST /analysis-jobs 提交（同一 request_key 幂等），GET /analysis-jobs/{id} 查进度，POST /analysis-jobs/{id}/cancel 取消，POST /analysis-jobs/{id}/retry 重试。
工作进程：python -m autumn_ledger worker --database autumn-ledger.sqlite3（--once 处理完当前队列即退出，可多进程竞争领取）。
作业提交时固定规范化请求与数据版本；运行期间新导入的数据属于下一份报告，故障后从已完成的统计窗口续跑。
合成业务演练：python -B -m autumn_ledger demo
测试：python -B -m unittest discover -s tests -v
构建：python -B -m autumn_ledger.check_source
数据完整性：python -B -m autumn_ledger check --database autumn-ledger.sqlite3
金额单位为人民币分；订单修订为全量快照，退款为累计值。
出游日期是供应商给出的目的地本地日期；事件时间必须含时区。
取消不计出游人数，未退收入仍计入净额。缺报不等于零需求。
本地演练使用虚构数据，分析仅描述所选来源样本。

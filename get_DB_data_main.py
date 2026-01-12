# -*- coding: utf-8 -*-
"""
项目主入口文件
完全按照任务要求中的 config 格式调用
"""
import dataGet_DB_AmazingData

# 配置字典 (参考任务要求文档格式)
config = {
    "param_dataSrc": [
        {
            "func_name_dataSrc": "main",  # 指向 AmazingData 的 main

            # 因子源数据输入字段配置 (此次任务主要用于生成基础映射表)
            "colName_dataSrc": [],

            # 标的配置
            "security": ['stock'],

            # 数据库表名
            "DB_tb_name": ["stock_basic", "daily"],

            # 数据库路径
            "DB_dir": ["DB_QMT/"],

            # 源数据频率
            "dataSrc_frequency": "daily",

            # 获取模式
            "dataSrc_getMode": "offline",

            # 保存模式
            "dataSrc_saveMode": "overwrite",

            # 多进程
            "dataSrc_multiProcess": False,

            # 时间范围
            "dataSrc_start_date": "2025-01-01",
            "dataSrc_end_date": "2025-06-01"
        }
    ]
}

if __name__ == "__main__":
    # 取最后一个配置项进行执行 (参考文档逻辑)
    param = config["param_dataSrc"][-1]

    print("开始执行 AmazingData 数据任务...")
    # 调用 dataGet_DB_AmazingData 的 main 函数
    df = dataGet_DB_AmazingData.main(param)
    print("执行完成。")
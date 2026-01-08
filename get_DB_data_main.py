# ===============================================
# 银河证券 AmazingData 数据获取主程序
# ===============================================

config = {
    "param_dataSrc": [
        {
            "func_name_dataSrc": "query_DB_yh",

            # 列名配置：使用 AmazingData 手册中的英文字段名
            # K线接口(query_kline) 常见返回: open, close, high, low, volume, amount
            "colName_dataSrc": [
                {'date': 'date'},
                "open", "close", "high", "low", "volume",
                {'ts_code': 'market_code'}
            ],

            # 标的列表：请确保使用银河支持的后缀 (如 .SZ, .SH)
            "security": ["000001.SZ", "600519.SH"],

            # 频率
            "dataSrc_frequency": "daily",

            # 【重要】在这里填入您的 MAC 地址
            "ACCOUNT": "E0-0A-F6-55-30-DD",

            # 其他配置
            "verify_permission": False,
            "DB_tb_name": [],
            "dataSrc_outputFormat": "bz2",
            "dataSrc_start_date": "2023-01-01",
            "dataSrc_end_date": "2023-01-31",
            "dataSrc_multiProcess": False,
            "DB_dir": "DB_yh",
            "dataSrc_getMode": "online",
            "dataSrc_saveMode": "incremental_append",
        }
    ]
}

# ================= 调用执行 =================
from dataGet_DB_yh import query_securities

param = config["param_dataSrc"][-1]

# 提取 MAC 地址
mac_address = param.get("ACCOUNT", "")
if not mac_address or mac_address == "00:11:22:33:44:55":
    print("警告：请在 config['ACCOUNT'] 中填入您真实的 MAC 地址！")

# 启动采集
df = query_securities(
    security=param['security'],
    colName_dataSrc=param['colName_dataSrc'],
    dataSrc_frequency=param['dataSrc_frequency'],
    DB_tb_name=param['DB_tb_name'],
    DB_dir=param['DB_dir'],
    dataSrc_getMode=param['dataSrc_getMode'],
    dataSrc_saveMode=param['dataSrc_saveMode'],
    dataSrc_start_date=param['dataSrc_start_date'],
    dataSrc_end_date=param['dataSrc_end_date'],
    verify_permission=param['verify_permission'],
    dataSrc_multiProcess=param['dataSrc_multiProcess'],
    dataSrc_outputFormat=param['dataSrc_outputFormat'],
    host_mac=mac_address
)

print("\n=== 获取结果预览 ===")
print(df)
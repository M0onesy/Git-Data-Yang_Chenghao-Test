# ==============================================================================
# 银河证券(AmazingData) 数据采集配置文件
# ==============================================================================

# 1. 频率映射表
# 将 main 中的 'daily' 映射为银河 API 识别的参数
# 注意：根据手册 period 参数通常为字符串或枚举，这里暂定为字符串 '1d'
freq_mapping = {
    "daily": "1d",
    "weekly": "1w",
    "monthly": "1M",
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "60m": "60m",
    "tick": "tick"
}

DB_tb_field_supplement = {}

# 2. API 函数与参数配置
# 严格对应《AmazingData开发手册》中的接口名
DB_tb_API_inputArgs = {
    # --- 历史 K 线接口 (手册 1.1.4.2) ---
    "query_kline": {
        "code_list": [],  # 标的列表 [str]
        "period": ["1d"],  # 周期
        "begin_date": [],  # int: 20230101
        "end_date": [],  # int: 20230131
        # "begin_time": [],       # 可选
        # "end_time": []          # 可选
    },

    # --- 历史快照接口 (手册 1.1.4.1) ---
    "query_snapshot": {
        "code_list": [],
        "begin_date": [],
        "end_date": []
        # "begin_time": [],
        # "end_time": []
    },

    # --- 证券基础信息 (手册 1.1.2.9) ---
    "get_stock_basic": {
        "code_list": []
    },

    # --- 交易日历 (手册 1.1.2.8) ---
    "get_calendar": {
        # 根据手册此接口可能不需要必填参数，预留
    }
}

# 3. 品种与 API 的绑定关系
type_tb_func_mapping = {
    "stock": {
        'get_quote': 'query_snapshot',  # 行情/快照
        'get_kline_serial': 'query_kline',  # K线
        'get_tick_serial': 'query_snapshot',  # 暂用快照代替Tick
        "query_symbol_info": "get_stock_basic",
        "get_trading_calendar": "get_calendar"
    },
    "index": {
        'get_quote': 'query_snapshot',
        'get_kline_serial': 'query_kline',
        "query_symbol_info": "get_stock_basic",
        "get_trading_calendar": "get_calendar"
    },
    "future": {
        'get_quote': 'query_snapshot',
        'get_kline_serial': 'query_kline',
        "query_symbol_info": "get_stock_basic",
        "get_trading_calendar": "get_calendar"
    }
}

# 4. 获取限制配置
DB_tb_getLimit = {
    "query_kline": {"quantity": 1},  # 每次查1个标的
    "query_snapshot": {"quantity": 1},
    "get_stock_basic": {"quantity": 1000},
    "get_calendar": {}
}
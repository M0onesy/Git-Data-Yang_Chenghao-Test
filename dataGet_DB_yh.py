import time
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import concurrent.futures

# ==========================================
# 导入银河证券 AmazingData SDK
# ==========================================
# ==========================================
# 导入银河证券 AmazingData SDK (智能兼容模式)
# ==========================================
get_AmazingData_api = None
try:
    # 优先尝试 pip 安装后的标准包名
    from AmazingData import get_AmazingData_api
    print("成功导入标准包 AmazingData")
except ImportError as e1:
    try:
        # 再次尝试本地 pyd 文件名
        from api_AmazingData_professional import get_AmazingData_api
        print("成功导入本地文件 api_AmazingData_professional")
    except ImportError as e2:
        print("="*50)
        print(f"【SDK导入失败】\n尝试 AmazingData 报错: {e1}\n尝试 api_AmazingData_professional 报错: {e2}")
        print("请检查：\n1. 是否运行 pip install AmazingData...whl？\n2. 文件夹下是否有 .pyd 文件？")
        print("="*50)
        # 不要在这里 raise，让后面函数抛出更具体的错

from dataGet_DB_yh_const import (DB_tb_getLimit, DB_tb_API_inputArgs, type_tb_func_mapping, freq_mapping)

# 全局 API 单例
global_ad = None


def get_yh_api(host_mac=None):
    """
    初始化 AmazingData API 连接
    :param host_mac: 用户的 MAC 地址
    """
    global global_ad
    if global_ad is None:
        if get_AmazingData_api is None:
            raise ImportError("SDK导入失败")
        try:
            print(f"正在连接 AmazingData (MAC: {host_mac})...")
            # 使用您提供的 MAC 地址登录方式
            global_ad = get_AmazingData_api(host=host_mac)
            print("AmazingData 连接成功:", global_ad)
        except Exception as e:
            print(f"AmazingData 连接失败: {e}")
            raise e
    return global_ad


def record_symbol_time(symbol, start_time, end_time, DB_dir, status='success', api_name=''):
    """记录耗时日志"""
    duration = end_time - start_time
    record = {
        'symbol': symbol,
        'api_name': api_name,
        'start_time': datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S'),
        'duration_seconds': round(duration, 2),
        'status': status,
        'record_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    csv_file = f'{DB_dir}/record_time.csv'
    os.makedirs(DB_dir, exist_ok=True)
    if os.path.exists(csv_file):
        try:
            pd.DataFrame([record]).to_csv(csv_file, mode='a', header=False, index=False)
        except:
            pass
    else:
        pd.DataFrame([record]).to_csv(csv_file, index=False)
    print(f"[{symbol}] {api_name} {status}，耗时: {duration:.2f}秒")


def get_security_type_all(DB_dir):
    """
    获取全量标的列表
    """
    file_path = f"{DB_dir}/security_type_all.csv"
    if os.path.exists(file_path):
        return pd.read_csv(file_path)

    print("正在初始化标的列表...")
    # 尝试调用接口获取，如果失败则创建一个空表供后续填充
    try:
        if hasattr(global_ad, 'get_code_info'):
            # 1.1.2.1 每日最新证券信息
            df = global_ad.get_code_info(security_type='EXTRA_STOCK_A')
            if df is not None and not df.empty:
                df = df.reset_index()
                # 假设返回的 index 是代码
                if 'index' in df.columns:
                    df.rename(columns={'index': 'symbol'}, inplace=True)
                df['type'] = 'stock'
                os.makedirs(DB_dir, exist_ok=True)
                df.to_csv(file_path, index=False)
                return df
    except Exception as e:
        print(f"自动获取标的列表失败(可忽略): {e}")

    # 兜底
    columns = ["symbol", "type", "name"]
    df = pd.DataFrame(columns=columns)
    os.makedirs(DB_dir, exist_ok=True)
    df.to_csv(file_path, index=False)
    return df


def get_tb_structure_all(DB_dir, security_type_all):
    """
    探测表结构：通过试探性请求获取API返回的列名
    """
    if os.path.exists(f"{DB_dir}/tb_structure_all.csv"):
        return pd.read_csv(f"{DB_dir}/tb_structure_all.csv")

    print("正在探测 AmazingData 表结构...")
    tb_structure_all = pd.DataFrame(
        columns=['名称', '类型', '数据来源', '频率', '源数据映射', '是否默认获取', 'type', "country_region"])

    test_targets = {'stock': '000001.SZ'}

    # 准备测试时间 int YYYYMMDD
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=10)
    start_int = int(start_dt.strftime("%Y%m%d"))
    end_int = int(end_dt.strftime("%Y%m%d"))

    for symbol_type, func_map in type_tb_func_mapping.items():
        test_symbol = test_targets.get(symbol_type, '000001.SZ')
        api_names = set(func_map.values())

        for api_name in api_names:
            if not hasattr(global_ad, api_name):
                continue

            try:
                func = getattr(global_ad, api_name)
                res = None

                # 构造试探参数
                if api_name == "query_kline":
                    # 手册 1.1.4.2
                    res_dict = func(code_list=[test_symbol], begin_date=start_int, end_date=end_int, period="1d")
                    if res_dict and test_symbol in res_dict:
                        res = res_dict[test_symbol]

                elif api_name == "get_stock_basic":
                    # 手册 1.1.2.9
                    res = func(code_list=[test_symbol])

                if res is not None and not res.empty:
                    rows = [[str(col), 'float', api_name, 'daily', None, True, symbol_type, "China"] for col in
                            res.columns]
                    tb_structure_all = pd.concat(
                        [tb_structure_all, pd.DataFrame(rows, columns=tb_structure_all.columns)], ignore_index=True)
            except Exception as e:
                pass

    tb_structure_all.to_csv(f"{DB_dir}/tb_structure_all.csv", index=False)
    print("表结构构建完成。")
    return tb_structure_all


def parse_colName_dict(colName_dataSrc):
    rename_all_dict = {}
    for colName_item in colName_dataSrc:
        if isinstance(colName_item, dict):
            for new_col, value in colName_item.items():
                if isinstance(value, list) or isinstance(value, dict):
                    if isinstance(value, dict):
                        for k, v in value.items(): rename_all_dict[v] = new_col
                    elif isinstance(value, list):
                        for v in value:
                            if isinstance(v, str): rename_all_dict[v] = new_col
                elif isinstance(value, str):
                    rename_all_dict[value] = new_col
        elif isinstance(colName_item, str):
            rename_all_dict[colName_item] = colName_item
    return rename_all_dict


def get_tb_list_by_factor_colName(column_name_list, symbol_type, dataSrc_frequency, tb_structure_all, start_dt, end_dt):
    """匹配 API 并构造参数"""
    intputArgs_dict = {}

    yh_freq = freq_mapping.get(dataSrc_frequency, "1d")

    # 必须转换为 int
    start_int = int(start_dt.strftime("%Y%m%d"))
    end_int = int(end_dt.strftime("%Y%m%d"))

    # 兜底：默认查 K 线
    if tb_structure_all.empty:
        intputArgs_dict["query_kline"] = {
            "period": yh_freq,
            "begin_date": start_int,
            "end_date": end_int
        }
        return intputArgs_dict

    for column_name in column_name_list:
        rows = tb_structure_all[(tb_structure_all['type'] == symbol_type) & (tb_structure_all['名称'] == column_name)]
        if not rows.empty:
            apis = rows['数据来源'].unique()
            for api in apis:
                if api not in intputArgs_dict:
                    # 根据 const 配置构造参数
                    if api == "query_kline":
                        intputArgs_dict[api] = {
                            "period": yh_freq,
                            "begin_date": start_int,
                            "end_date": end_int
                        }
                    elif api == "query_snapshot":
                        intputArgs_dict[api] = {
                            "begin_date": start_int,
                            "end_date": end_int
                        }
                    else:
                        intputArgs_dict[api] = {}

    if not intputArgs_dict:
        intputArgs_dict["query_kline"] = {
            "period": yh_freq,
            "begin_date": start_int,
            "end_date": end_int
        }
    return intputArgs_dict


def get_tb_api_data(api_name, intputArgs, symbol):
    """执行 API 调用"""
    try:
        if not hasattr(global_ad, api_name):
            return None

        func = getattr(global_ad, api_name)
        kwargs = intputArgs.copy()

        # AmazingData 参数 code_list 要求是列表
        kwargs['code_list'] = [symbol]

        # 执行调用
        result = func(**kwargs)

        # 解析返回结果
        df = None
        # query_kline 等接口返回的是 dict: {code: dataframe}
        if isinstance(result, dict):
            if symbol in result:
                df = result[symbol]
        elif isinstance(result, pd.DataFrame):
            df = result

        if df is not None and not df.empty:
            df = df.reset_index()
            # 转小写
            df.columns = [str(col).lower() for col in df.columns]

            # 标准化日期和代码列名
            # 手册中 index 为 datetime, 转换后可能是 'index'
            rename_map = {
                'index': 'date',
                'trade_date': 'date',
                'market_code': 'ts_code'
            }
            df.rename(columns=rename_map, inplace=True)
            return df

    except Exception as e:
        print(f"[{symbol}] API调用异常 ({api_name}): {e}")
    return None


def save_data(df_result, symbol, DB_dir, symbol_type, api_name, dataSrc_saveMode, dataSrc_outputFormat):
    """数据保存"""
    if df_result is None or df_result.empty: return None

    result = df_result.copy()
    save_path = os.path.join(DB_dir, symbol_type.upper(), api_name)
    os.makedirs(save_path, exist_ok=True)
    file_path = os.path.join(save_path, f"{symbol}.csv.{dataSrc_outputFormat}")

    try:
        if dataSrc_saveMode == 'incremental_append' and os.path.exists(file_path):
            old_data = pd.read_pickle(file_path)
            # 简单的日期去重逻辑
            if 'date' in result.columns and 'date' in old_data.columns:
                if not pd.api.types.is_datetime64_any_dtype(result['date']):
                    result['date'] = pd.to_datetime(result['date'])
                if not pd.api.types.is_datetime64_any_dtype(old_data['date']):
                    old_data['date'] = pd.to_datetime(old_data['date'])

                if not old_data.empty:
                    last_dt = old_data['date'].max()
                    result = result[result['date'] > last_dt]

            if not result.empty:
                pd.concat([old_data, result], ignore_index=True).to_pickle(file_path, compression='bz2')
        else:
            result.to_pickle(file_path, compression='bz2')
        return result
    except Exception as e:
        print(f"保存失败 {symbol}: {e}")
        return None


def concat_or_merge_data(final_df, new_df, rename_dict=None):
    """合并不同接口的数据"""
    if new_df is None or new_df.empty: return final_df
    temp = new_df.copy()

    if rename_dict:
        temp.rename(columns=rename_dict, inplace=True)

    if "date" in temp.columns:
        temp['date'] = pd.to_datetime(temp["date"])

    if final_df.empty: return temp

    keys = list(set(final_df.columns) & set(temp.columns))
    if 'date' in keys:
        merge_keys = ['date']
        if 'ts_code' in keys: merge_keys.append('ts_code')
        try:
            return pd.merge(final_df, temp, on=merge_keys, how='outer')
        except:
            return pd.concat([final_df, temp], axis=0, ignore_index=True)
    else:
        return pd.concat([final_df, temp], axis=0, ignore_index=True)


def query_DB(security, security_type_all, tb_structure_all, colName_dataSrc, dataSrc_frequency,
             DB_dir, dataSrc_saveMode, dataSrc_start_date, dataSrc_end_date, dataSrc_outputFormat):
    start_dt = pd.to_datetime(dataSrc_start_date) if dataSrc_start_date else datetime.now() - timedelta(365)
    end_dt = pd.to_datetime(dataSrc_end_date) if dataSrc_end_date else datetime.now()

    # 简易类型推断
    symbol_type = 'stock'
    if security.startswith('000') and (security.endswith('.SH') or security.endswith('.SZ')):
        symbol_type = 'index'

    rename_dict = parse_colName_dict(colName_dataSrc)
    input_args = get_tb_list_by_factor_colName(list(rename_dict.keys()), symbol_type, dataSrc_frequency,
                                               tb_structure_all, start_dt, end_dt)

    res_df = pd.DataFrame()
    for api_name, args in input_args.items():
        t0 = time.time()
        status = 'fail'

        df = get_tb_api_data(api_name, args, security)
        if df is not None and not df.empty:
            df = save_data(df, security, DB_dir, symbol_type, api_name, dataSrc_saveMode, dataSrc_outputFormat)
            if df is not None:
                if 'ts_code' not in df.columns: df['ts_code'] = security
                res_df = concat_or_merge_data(res_df, df, rename_dict)
                status = 'success'

        record_symbol_time(security, t0, time.time(), DB_dir, status, api_name)

    return res_df, rename_dict


def query_securities(security, colName_dataSrc=[], dataSrc_frequency='daily', DB_tb_name=[], DB_dir='DB_yh/',
                     dataSrc_getMode='offline', dataSrc_saveMode='overwrite', dataSrc_start_date='',
                     dataSrc_end_date='', verify_permission=False, dataSrc_multiProcess=False,
                     dataSrc_outputFormat='bz2',
                     host_mac=None):
    # 初始化
    get_yh_api(host_mac)

    final_res = pd.DataFrame()
    sec_list = security if isinstance(security, list) else [security]

    sec_type_all = get_security_type_all(DB_dir)
    tb_struct_all = get_tb_structure_all(DB_dir, sec_type_all)

    # 循环
    if not dataSrc_multiProcess:
        for sec in sec_list:
            res, _ = query_DB(sec, sec_type_all, tb_struct_all, colName_dataSrc, dataSrc_frequency,
                              DB_dir, dataSrc_saveMode, dataSrc_start_date, dataSrc_end_date, dataSrc_outputFormat)
            final_res = concat_or_merge_data(final_res, res)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(query_DB, sec, sec_type_all, tb_struct_all, colName_dataSrc,
                                       dataSrc_frequency, DB_dir, dataSrc_saveMode,
                                       dataSrc_start_date, dataSrc_end_date, dataSrc_outputFormat): sec for sec in
                       sec_list}
            for f in concurrent.futures.as_completed(futures):
                try:
                    res, _ = f.result()
                    final_res = concat_or_merge_data(final_res, res)
                except Exception as e:
                    print(f"Error: {e}")

    return final_res
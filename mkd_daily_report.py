#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MKD每日数据报表生成系统
整合版本 - 包含所有功能
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import os
import sys
import warnings
import pymysql
from sqlalchemy import create_engine, text
warnings.filterwarnings('ignore')

# 数据库配置
DB_CONFIG = {
    'host': '124.220.21.165',
    'port': 3306,
    'user': 'wufushen',
    'password': 'Wufushen123..',
    'database': 'daily_data',
    'charset': 'utf8mb4'
}

# ===================== 核心功能函数 =====================

def find_latest_files():
    """查找source目录下最新的数据文件"""
    files = glob.glob('source/*.xlsx')
    # 过滤掉临时文件（以~$开头的文件）
    files = [f for f in files if not os.path.basename(f).startswith('~$')]
    inventory_file = [f for f in files if '库存管理' in f]
    profit_file = [f for f in files if '利润分析' in f]
    order_file = [f for f in files if '订单管理' in f]

    if not inventory_file:
        raise FileNotFoundError("未找到库存管理文件")
    if not profit_file:
        raise FileNotFoundError("未找到利润分析文件")
    if not order_file:
        raise FileNotFoundError("未找到订单管理文件")

    return inventory_file[0], profit_file[0], order_file[0]

def read_data_files():
    """读取数据文件，只读取需要的列"""
    inventory_file, profit_file, order_file = find_latest_files()

    print(f"读取库存管理文件: {os.path.basename(inventory_file)}")
    inventory_cols = ['商品ID', '商品SKU', '可用库存', '近7天销量', '近15天销量', '近30天销量', '近60天销量']
    df_inventory = pd.read_excel(inventory_file, usecols=inventory_cols)

    print(f"读取利润分析文件: {os.path.basename(profit_file)}")
    profit_cols = ['商品ID', '净利率', 'ACoAS']
    df_profit = pd.read_excel(profit_file, usecols=profit_cols)

    print(f"读取订单管理文件: {os.path.basename(order_file)}")
    order_cols = ['订单日期', '订单状态', '商品ID', 'SKU', '销售数量', '销售额(MXN)']
    df_orders = pd.read_excel(order_file, usecols=order_cols)

    return df_inventory, df_profit, df_orders

def extract_seller_spu(sku):
    """从SKU提取卖家SPU（去掉最后一个-及其后面的部分）"""
    if pd.isna(sku):
        return ''
    sku_str = str(sku)
    parts = sku_str.rsplit('-', 1)
    if len(parts) > 1:
        return parts[0]
    return sku_str

def calculate_daily_sales_avg(row):
    """计算日均销量：0.6*近7天销量/7+0.4*近15天销量/15"""
    sales_7d = float(row.get('近7天销量', 0) or 0)
    sales_15d = float(row.get('近15天销量', 0) or 0)
    return 0.6 * sales_7d / 7 + 0.4 * sales_15d / 15

def process_inventory_data(df_inventory):
    """处理库存数据"""
    df = df_inventory.copy()
    df.rename(columns={
        '商品ID': '平台SPU',
        '商品SKU': '卖家SKU',
        '可用库存': '在售库存'
    }, inplace=True)

    # 生成卖家SPU
    df['卖家SPU'] = df['卖家SKU'].apply(extract_seller_spu)

    # 生成平台SPU&卖家SKU（直接连接，不加&符号）
    df['平台SPU&卖家SKU'] = df['平台SPU'].astype(str) + df['卖家SKU'].astype(str)

    # 计算日均销量
    df['日均销量'] = df.apply(calculate_daily_sales_avg, axis=1)

    # 计算可售天数（避免除零）
    df['可售天数'] = df.apply(
        lambda row: row['在售库存'] / row['日均销量'] if row['日均销量'] > 0 else 0,
        axis=1
    )

    return df

def merge_profit_data(df_base, df_profit):
    """合并利润数据"""
    # 去重，保留第一条记录
    df_profit_unique = df_profit.drop_duplicates(subset=['商品ID'], keep='first')

    # 将百分比转换为小数
    def percent_to_decimal(value):
        if pd.isna(value) or value == '' or value is None or value == '-':
            return 0.00
        if isinstance(value, str):
            if '%' in value:
                # 移除%号并转换
                value = value.replace('%', '')
                if value == '' or value == '-':
                    return 0.00
                return float(value) / 100
            elif value == '-':
                return 0.00
        try:
            return float(value)
        except:
            return 0.00

    df_profit_unique['净利率'] = df_profit_unique['净利率'].apply(percent_to_decimal)
    df_profit_unique['ACoAS'] = df_profit_unique['ACoAS'].apply(percent_to_decimal)

    # 重命名列
    df_profit_unique.rename(columns={
        '商品ID': '平台SPU',
        '净利率': '近7天净利率',
        'ACoAS': '近7天ACoAS'
    }, inplace=True)

    # 合并数据
    df_merged = pd.merge(df_base, df_profit_unique, on='平台SPU', how='left')

    # 填充缺失值
    df_merged['近7天净利率'] = df_merged['近7天净利率'].fillna(0.00)
    df_merged['近7天ACoAS'] = df_merged['近7天ACoAS'].fillna(0.00)

    return df_merged

def calculate_daily_metrics(df_orders):
    """计算每日销量和销售额"""
    # 筛选已支付订单
    df_paid = df_orders[df_orders['订单状态'] == '已支付'].copy()

    # 转换订单日期为datetime
    df_paid['订单日期'] = pd.to_datetime(df_paid['订单日期'])

    # 获取今天的日期
    today = datetime.now().date()

    # 创建一个字典存储每个商品ID+SKU组合的每日数据
    daily_metrics = {}

    for days_ago in range(1, 8):
        target_date = today - timedelta(days=days_ago)

        # 筛选特定日期的订单
        df_day = df_paid[df_paid['订单日期'].dt.date == target_date]

        if not df_day.empty:
            # 按商品ID和SKU分组统计
            grouped = df_day.groupby(['商品ID', 'SKU']).agg({
                '销售数量': 'sum',
                '销售额(MXN)': 'sum'
            }).reset_index()

            for _, row in grouped.iterrows():
                key = (row['商品ID'], row['SKU'])
                if key not in daily_metrics:
                    daily_metrics[key] = {}

                daily_metrics[key][f'{days_ago}天前销售额'] = round(row['销售额(MXN)'], 2)
                daily_metrics[key][f'{days_ago}天前销量'] = int(row['销售数量'])

    return daily_metrics

def merge_daily_metrics(df_base, daily_metrics):
    """合并每日指标到基础数据"""
    # 创建每日列
    for days_ago in range(1, 8):
        df_base[f'{days_ago}天前销量'] = 0
        df_base[f'{days_ago}天前销售额'] = 0.00

    # 填充数据
    for idx, row in df_base.iterrows():
        key = (row['平台SPU'], row['卖家SKU'])
        if key in daily_metrics:
            for metric_name, value in daily_metrics[key].items():
                df_base.at[idx, metric_name] = value

    return df_base

def format_output_data(df):
    """格式化输出数据"""
    # 定义输出列顺序（按照PRD要求）
    output_columns = [
        '平台SPU', '卖家SKU', '卖家SPU', '平台SPU&卖家SKU', '近60天销量', '近30天销量', '近15天销量',
        '近7天销量', '日均销量', '可售天数', '近7天净利率', '近7天ACoAS', '在售库存',
        '7天前销量', '6天前销量', '5天前销量', '4天前销量', '3天前销量', '2天前销量', '1天前销量',
        '7天前销售额', '6天前销售额', '5天前销售额', '4天前销售额', '3天前销售额', '2天前销售额', '1天前销售额'
    ]

    # 确保所有列都存在
    for col in output_columns:
        if col not in df.columns:
            if '销量' in col:
                df[col] = 0
            elif '销售额' in col or '净利率' in col or 'ACoAS' in col:
                df[col] = 0.00
            else:
                df[col] = ''

    # 选择输出列
    df_output = df[output_columns].copy()

    # 格式化数值
    sales_cols = [col for col in df_output.columns if '销量' in col and col != '日均销量']
    for col in sales_cols:
        df_output[col] = df_output[col].apply(lambda x: int(x) if not pd.isna(x) else 0)

    sales_amount_cols = [col for col in df_output.columns if '销售额' in col]
    for col in sales_amount_cols:
        df_output[col] = df_output[col].apply(lambda x: round(float(x), 2) if not pd.isna(x) else 0.00)

    # 处理净利率和ACoAS字段，保留4位小数
    if '近7天净利率' in df_output.columns:
        df_output['近7天净利率'] = df_output['近7天净利率'].apply(lambda x: round(float(x), 4) if not pd.isna(x) else 0.0000)
    if '近7天ACoAS' in df_output.columns:
        df_output['近7天ACoAS'] = df_output['近7天ACoAS'].apply(lambda x: round(float(x), 4) if not pd.isna(x) else 0.0000)

    df_output['可售天数'] = df_output['可售天数'].apply(lambda x: round(float(x), 2) if not pd.isna(x) else 0.00)
    df_output['日均销量'] = df_output['日均销量'].apply(lambda x: round(float(x), 1) if not pd.isna(x) else 0.0)

    # 处理空值，确保卖家SKU不为空
    df_output['卖家SKU'] = df_output['卖家SKU'].fillna('')

    # 去除重复行（基于平台SPU和卖家SKU的组合）
    df_output = df_output.drop_duplicates(subset=['平台SPU', '卖家SKU'], keep='first')

    print(f"  去重后剩余: {len(df_output)} 条记录")

    return df_output

# ===================== 数据库操作函数 =====================

def create_database_connection():
    """创建数据库连接"""
    try:
        engine = create_engine(
            f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            f"?charset={DB_CONFIG['charset']}"
        )
        return engine
    except Exception as e:
        print(f"  数据库连接失败: {str(e)}")
        return None

def create_table_if_not_exists(engine):
    """创建表（如果不存在）"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS daily_data_mkd (
        id INT AUTO_INCREMENT PRIMARY KEY,
        platform_spu VARCHAR(20) NOT NULL COMMENT '平台SPU',
        seller_sku VARCHAR(50) NOT NULL COMMENT '卖家SKU',
        platform_seller_sku VARCHAR(100) COMMENT '平台SPU&卖家SKU',
        seller_spu VARCHAR(50) COMMENT '卖家SPU',
        sales_60d INT DEFAULT 0 COMMENT '近60天销量',
        sales_30d INT DEFAULT 0 COMMENT '近30天销量',
        sales_15d INT DEFAULT 0 COMMENT '近15天销量',
        sales_7d INT DEFAULT 0 COMMENT '近7天销量',
        avg_daily_sales DECIMAL(10,1) DEFAULT 0.0 COMMENT '日均销量',
        available_stock INT DEFAULT 0 COMMENT '在售库存',
        sellable_days DECIMAL(10,2) DEFAULT 0.00 COMMENT '可售天数',
        profit_rate_7d DECIMAL(10,4) DEFAULT 0.0000 COMMENT '近7天净利率',
        acoas_7d DECIMAL(10,4) DEFAULT 0.0000 COMMENT '近7天ACoAS',
        sales_7d_ago INT DEFAULT 0 COMMENT '7天前销量',
        sales_6d_ago INT DEFAULT 0 COMMENT '6天前销量',
        sales_5d_ago INT DEFAULT 0 COMMENT '5天前销量',
        sales_4d_ago INT DEFAULT 0 COMMENT '4天前销量',
        sales_3d_ago INT DEFAULT 0 COMMENT '3天前销量',
        sales_2d_ago INT DEFAULT 0 COMMENT '2天前销量',
        sales_1d_ago INT DEFAULT 0 COMMENT '1天前销量',
        gmv_7d_ago DECIMAL(10,2) DEFAULT 0.00 COMMENT '7天前GMV',
        gmv_6d_ago DECIMAL(10,2) DEFAULT 0.00 COMMENT '6天前GMV',
        gmv_5d_ago DECIMAL(10,2) DEFAULT 0.00 COMMENT '5天前GMV',
        gmv_4d_ago DECIMAL(10,2) DEFAULT 0.00 COMMENT '4天前GMV',
        gmv_3d_ago DECIMAL(10,2) DEFAULT 0.00 COMMENT '3天前GMV',
        gmv_2d_ago DECIMAL(10,2) DEFAULT 0.00 COMMENT '2天前GMV',
        gmv_1d_ago DECIMAL(10,2) DEFAULT 0.00 COMMENT '1天前GMV',
        data_date DATE COMMENT '数据日期',
        INDEX idx_platform_spu (platform_spu),
        INDEX idx_seller_sku (seller_sku),
        INDEX idx_data_date (data_date),
        UNIQUE KEY uk_sku_date (platform_spu, seller_sku, data_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='MKD每日数据表'
    """

    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print("  数据表检查/创建成功")
        return True
    except Exception as e:
        print(f"  创建表失败: {str(e)}")
        return False

def upload_to_database(df_output, engine):
    """上传数据到数据库（使用TRUNCATE清空表）"""
    try:
        df_upload = df_output.copy()

        # 重命名列以匹配数据库字段
        column_mapping = {
            '平台SPU': 'platform_spu',
            '卖家SKU': 'seller_sku',
            '卖家SPU': 'seller_spu',
            '近60天销量': 'sales_60d',
            '近30天销量': 'sales_30d',
            '近15天销量': 'sales_15d',
            '近7天销量': 'sales_7d',
            '日均销量': 'avg_daily_sales',
            '在售库存': 'available_stock',
            '可售天数': 'sellable_days',
            '近7天净利率': 'profit_rate_7d',
            '近7天ACoAS': 'acoas_7d',
            '7天前销量': 'sales_7d_ago',
            '6天前销量': 'sales_6d_ago',
            '5天前销量': 'sales_5d_ago',
            '4天前销量': 'sales_4d_ago',
            '3天前销量': 'sales_3d_ago',
            '2天前销量': 'sales_2d_ago',
            '1天前销量': 'sales_1d_ago',
            '平台SPU&卖家SKU': 'platform_seller_sku',
            '7天前销售额': 'gmv_7d_ago',
            '6天前销售额': 'gmv_6d_ago',
            '5天前销售额': 'gmv_5d_ago',
            '4天前销售额': 'gmv_4d_ago',
            '3天前销售额': 'gmv_3d_ago',
            '2天前销售额': 'gmv_2d_ago',
            '1天前销售额': 'gmv_1d_ago'
        }

        df_upload.rename(columns=column_mapping, inplace=True)

        # 处理空值
        df_upload['seller_sku'] = df_upload['seller_sku'].fillna('')
        df_upload['platform_spu'] = df_upload['platform_spu'].fillna('')
        df_upload['seller_spu'] = df_upload['seller_spu'].fillna('')

        # 添加数据日期
        df_upload['data_date'] = datetime.now().date()

        # 使用TRUNCATE清空整个表
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()
            # TRUNCATE比DELETE更快，会重置自增ID
            cursor.execute("TRUNCATE TABLE daily_data_mkd")
            conn.commit()
            cursor.close()
            conn.close()
            print("  已清空数据表")
        except Exception as e:
            print(f"  清空数据表时出现问题: {str(e)}")

        # 上传新数据
        df_upload.to_sql(
            name='daily_data_mkd',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=100
        )

        print(f"  成功上传 {len(df_upload)} 条数据到数据库")
        return True

    except Exception as e:
        print(f"  数据上传失败: {str(e)}")
        return False

# ===================== 主程序函数 =====================

def generate_report():
    """生成报表的主函数"""
    print("="*60)
    print("MKD每日数据报表生成")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    try:
        # 1. 读取数据
        print("\n[1/6] 读取数据文件...")
        df_inventory, df_profit, df_orders = read_data_files()
        print(f"  库存数据: {len(df_inventory)} 行")
        print(f"  利润数据: {len(df_profit)} 行")
        print(f"  订单数据: {len(df_orders)} 行")

        # 2. 处理库存数据
        print("\n[2/6] 处理库存数据...")
        df_base = process_inventory_data(df_inventory)
        print(f"  处理完成: {len(df_base)} 条记录")

        # 3. 合并利润数据
        print("\n[3/6] 合并利润数据...")
        df_base = merge_profit_data(df_base, df_profit)

        # 4. 计算每日销量和销售额
        print("\n[4/6] 计算每日销量和销售额...")
        daily_metrics = calculate_daily_metrics(df_orders)
        print(f"  统计SKU数量: {len(daily_metrics)}")

        # 5. 合并每日数据
        print("\n[5/6] 合并每日数据...")
        df_final = merge_daily_metrics(df_base, daily_metrics)

        # 6. 格式化并输出
        print("\n[6/6] 格式化并输出数据...")
        df_output = format_output_data(df_final)

        # 保存到Excel
        output_file = 'mkddaily.xlsx'
        try:
            df_output.to_excel(output_file, index=False, engine='openpyxl')
        except PermissionError:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'mkddaily_{timestamp}.xlsx'
            df_output.to_excel(output_file, index=False, engine='openpyxl')
            print(f"  注意: mkddaily.xlsx被占用，已保存为 {output_file}")

        print(f"\n[成功] 报表生成成功！")
        print(f"  输出文件: {output_file}")
        print(f"  数据行数: {len(df_output)}")

        # 7. 上传到数据库
        print("\n[7/7] 上传数据到数据库...")
        engine = create_database_connection()
        if engine:
            if create_table_if_not_exists(engine):
                upload_to_database(df_output, engine)
            engine.dispose()
        else:
            print("  跳过数据库上传（连接失败）")

        print("\n" + "="*60)
        print("处理完成！")

    except Exception as e:
        print(f"\n[错误] {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


def main():
    """主程序入口 - 直接执行生成报表"""
    return generate_report()

if __name__ == "__main__":
    exit(main())
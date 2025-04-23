import psycopg2
from decimal import Decimal
import time
from datetime import datetime

# 数据库配置
DB_CONFIG = {
    'host': '172.16.32.93',
    'port': '5432',
    'database': 'stock',
    'user': 'postgres',
    'password': '123456'
}

# 只计算最近20个交易日
TRADING_DAYS_LIMIT = 20

def get_db_connection():
    """获取数据库连接"""
    conn_string = f"host={DB_CONFIG['host']} port={DB_CONFIG['port']} dbname={DB_CONFIG['database']} user={DB_CONFIG['user']} password={DB_CONFIG['password']}"
    return psycopg2.connect(conn_string)

def get_latest_trading_dates(limit=TRADING_DAYS_LIMIT):
    """获取最近的交易日期列表
    
    参数:
    limit: 限制返回的交易日数量
    
    返回:
    list: 交易日期列表，按日期降序排序
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取最近的交易日期
        cursor.execute("""
            SELECT DISTINCT trade_date 
            FROM all_stocks_days 
            ORDER BY trade_date DESC 
            LIMIT %s
        """, (limit,))
        
        dates = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return dates
    except Exception as e:
        print(f"获取交易日期时出错: {str(e)}")
        return []

def get_all_stocks_count():
    """获取数据库中股票数量"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取股票总数
        cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM all_stocks_days")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return count
    except Exception as e:
        print(f"获取股票总数时出错: {str(e)}")
        return 0

def get_stocks_with_signals_from_db():
    """从数据库获取有买卖点信号的股票数据"""
    try:
        # 获取最近的交易日期
        print("正在获取最近的交易日期...")
        latest_dates = get_latest_trading_dates(TRADING_DAYS_LIMIT)
        if not latest_dates:
            print("❌ 错误: 无法获取最近交易日期")
            return {"error": "无法获取最近交易日期"}
        
        print(f"✓ 获取到最近{len(latest_dates)}个交易日，从 {latest_dates[0]} 到 {latest_dates[-1]}")
        
        # 获取数据库中股票总数
        total_stocks = get_all_stocks_count()
        print(f"数据库中共有 {total_stocks} 只股票")
        
        # 连接数据库
        print("正在连接数据库...")
        conn = get_db_connection()
        cursor = conn.cursor()
        print("✓ 数据库连接成功")
        
        # 查询有信号的股票（在最近20天内有买点或卖点的股票）
        print("正在查询有买卖点信号的股票...")
        cursor.execute("""
            SELECT DISTINCT a.ts_code 
            FROM all_stocks_days a
            JOIN high_level_inflows h ON a.id = h.all_stocks_days_id
            WHERE a.trade_date >= %s AND (h.buy > 0 OR h.sell > 0)
            ORDER BY a.ts_code
        """, (latest_dates[-1],))
        
        stocks_with_signals = [row[0] for row in cursor.fetchall()]
        
        if not stocks_with_signals:
            print("❌ 没有找到有信号的股票")
            cursor.close()
            conn.close()
            return {"error": "没有找到有信号的股票"}
        
        print(f"✓ 找到 {len(stocks_with_signals)} 只有买卖点信号的股票 (数据库总共 {total_stocks} 只股票)")
        
        # 构建结果数据
        print("正在获取股票详细数据...")
        column_names = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "bay", "ma120", "ma250", "name", "sell"]
        data = []
        stock_returns = []
        
        # 获取每只股票的数据
        for idx, ts_code in enumerate(stocks_with_signals):
            # 每处理一只股票都输出一次
            print(f"  正在加载第 {idx+1}/{len(stocks_with_signals)} 只股票数据: {ts_code}")
                
            # 获取股票最近20天数据，包括买卖点信息
            cursor.execute("""
                SELECT a.ts_code, a.trade_date, a.open, a.high, a.low, a.close, a.pre_close, 
                       a.pct_chg, a.vol, h.buy as bay, a.ma120, a.ma250, a.name, h.sell
                FROM all_stocks_days a
                LEFT JOIN high_level_inflows h ON a.id = h.all_stocks_days_id
                WHERE a.ts_code = %s AND a.trade_date >= %s
                ORDER BY a.trade_date
            """, (ts_code, latest_dates[-1]))
            
            stock_data = cursor.fetchall()
            
            if stock_data:
                # 转换数据类型
                stock_rows = []
                for row in stock_data:
                    stock_row = []
                    for j, item in enumerate(row):
                        if isinstance(item, Decimal):
                            stock_row.append(float(item))
                        elif item is None:
                            # 将NULL值转换为0.0
                            stock_row.append(0.0)
                        else:
                            stock_row.append(item)
                    stock_rows.append(stock_row)
                
                # 获取股票收益率
                cursor.execute("""
                    SELECT AVG(h.earnings_rate), COUNT(h.id)
                    FROM high_level_inflows h
                    JOIN all_stocks_days a ON h.all_stocks_days_id = a.id
                    WHERE a.ts_code = %s AND a.trade_date >= %s AND (h.buy > 0 OR h.sell > 0)
                """, (ts_code, latest_dates[-1]))
                
                avg_return = cursor.fetchone()
                avg_return_rate = float(avg_return[0]) if avg_return and avg_return[0] else 0.0
                signal_count = int(avg_return[1]) if avg_return and avg_return[1] else 0
                
                # 添加收益率信息
                stock_info = {
                    "ts_code": ts_code,
                    "name": stock_rows[0][12] if stock_rows and len(stock_rows[0]) > 12 else "",
                    "signal_count": signal_count,
                    "return_rate": avg_return_rate
                }
                stock_returns.append(stock_info)
                
                data.append(stock_rows)
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        
        # 按收益率排序股票
        print("正在按收益率排序股票...")
        stock_returns.sort(key=lambda x: x["return_rate"], reverse=True)
        
        # 构建结果
        result = {
            "column_names": column_names,
            "data": data,
            "page": 1,
            "stock_count": len(data),
            "total_stocks": total_stocks,
            "date_range": {
                "start": latest_dates[-1],
                "end": latest_dates[0],
                "days": len(latest_dates)
            },
            "stock_returns": stock_returns
        }
        
        print(f"✅ 数据加载完成！共加载了 {len(data)} 只股票的数据")
        return result
    except Exception as e:
        print(f"❌ 从数据库获取股票数据时出错: {str(e)}")
        return {"error": str(e)}

def get_single_stock_data(ts_code):
    """获取单只股票数据并计算买卖点"""
    try:
        # 获取最近的交易日期
        latest_dates = get_latest_trading_dates(TRADING_DAYS_LIMIT)
        if not latest_dates:
            return {"error": "无法获取最近交易日期"}
        
        # 连接数据库
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取股票数据
        cursor.execute("""
            SELECT a.ts_code, a.trade_date, a.open, a.high, a.low, a.close, a.pre_close, 
                   a.pct_chg, a.vol, h.buy as bay, a.ma120, a.ma250, a.name, h.sell
            FROM all_stocks_days a
            LEFT JOIN high_level_inflows h ON a.id = h.all_stocks_days_id
            WHERE a.ts_code = %s AND a.trade_date >= %s
            ORDER BY a.trade_date
        """, (ts_code, latest_dates[-1]))
        
        stock_data = cursor.fetchall()
        
        if not stock_data:
            cursor.close()
            conn.close()
            return {"error": f"没有找到股票 {ts_code} 的数据"}
        
        # 构建结果数据
        column_names = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "bay", "ma120", "ma250", "name", "sell"]
        
        # 转换数据类型
        stock_rows = []
        has_signal = False
        
        for row in stock_data:
            stock_row = []
            for j, item in enumerate(row):
                if isinstance(item, Decimal):
                    stock_row.append(float(item))
                elif item is None:
                    # 将NULL值转换为0.0
                    stock_row.append(0.0)
                else:
                    stock_row.append(item)
            
            if stock_row[9] > 0 or stock_row[-1] > 0:  # 检查买入或卖出信号 (bay或sell字段)
                has_signal = True
                
            stock_rows.append(stock_row)
        
        # 获取股票收益率
        cursor.execute("""
            SELECT AVG(h.earnings_rate), COUNT(h.id)
            FROM high_level_inflows h
            JOIN all_stocks_days a ON h.all_stocks_days_id = a.id
            WHERE a.ts_code = %s AND a.trade_date >= %s AND (h.buy > 0 OR h.sell > 0)
        """, (ts_code, latest_dates[-1]))
        
        avg_return = cursor.fetchone()
        avg_return_rate = float(avg_return[0]) if avg_return and avg_return[0] else 0.0
        signal_count = int(avg_return[1]) if avg_return and avg_return[1] else 0
        
        # 添加收益率信息
        stock_info = {
            "ts_code": ts_code,
            "name": stock_rows[0][12] if stock_rows and len(stock_rows[0]) > 12 else "",
            "signal_count": signal_count,
            "return_rate": avg_return_rate
        }
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        
        # 构建结果
        result = {
            "column_names": column_names,
            "data": [stock_rows],
            "page": 1,
            "stock_count": 1,
            "has_signal": has_signal,
            "date_range": {
                "start": latest_dates[-1],
                "end": latest_dates[0],
                "days": len(latest_dates)
            },
            "return_info": stock_info
        }
        
        return result
    except Exception as e:
        return {"error": str(e)}

def get_all_stocks_info():
    """获取数据库中所有股票的基本信息"""
    try:
        # 获取最近的交易日期
        latest_dates = get_latest_trading_dates(TRADING_DAYS_LIMIT)
        if not latest_dates:
            return {"error": "无法获取最近交易日期"}
        
        # 连接数据库
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 获取股票总数
        total_stocks = get_all_stocks_count()
        
        # 获取所有股票代码和名称
        cursor.execute("""
            SELECT DISTINCT ts_code, name 
            FROM all_stocks_days
            WHERE trade_date >= %s
            ORDER BY ts_code
        """, (latest_dates[-1],))
        
        stocks = [{"ts_code": row[0], "name": row[1]} for row in cursor.fetchall()]
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        
        result = {
            "stocks": stocks,
            "total_count": len(stocks),
            "database_total": total_stocks,
            "date_range": {
                "start": latest_dates[-1],
                "end": latest_dates[0],
                "days": len(latest_dates)
            }
        }
        
        return result
    except Exception as e:
        return {"error": str(e)}

def create_database_indexes():
    """创建数据库索引以提升查询性能"""
    try:
        print("正在创建数据库索引...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 使用事务来确保原子性操作
        conn.autocommit = False
        
        # 检查索引是否已存在，并尝试创建
        indexes_to_create = [
            {
                "name": "idx_all_stocks_days_ts_code",
                "table": "all_stocks_days",
                "columns": "ts_code",
                "description": "ts_code索引"
            },
            {
                "name": "idx_all_stocks_days_trade_date",
                "table": "all_stocks_days",
                "columns": "trade_date",
                "description": "trade_date索引"
            },
            {
                "name": "idx_all_stocks_days_ts_code_trade_date",
                "table": "all_stocks_days",
                "columns": "ts_code, trade_date",
                "description": "ts_code和trade_date复合索引"
            },
            {
                "name": "idx_high_level_inflows_all_stocks_days_id",
                "table": "high_level_inflows",
                "columns": "all_stocks_days_id",
                "description": "high_level_inflows外键索引"
            },
            {
                "name": "idx_high_level_inflows_signals",
                "table": "high_level_inflows",
                "columns": "buy, sell",
                "description": "买卖信号索引"
            }
        ]
        
        # 获取现有索引
        cursor.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE schemaname = 'public'
        """)
        existing_indexes = [row[0] for row in cursor.fetchall()]
        
        created_count = 0
        existing_count = 0
        
        # 创建不存在的索引
        for index in indexes_to_create:
            if index["name"] in existing_indexes:
                print(f"✓ {index['description']}已存在")
                existing_count += 1
                continue
            
            try:
                create_sql = f"CREATE INDEX {index['name']} ON {index['table']} ({index['columns']})"
                print(f"创建{index['description']}...")
                cursor.execute(create_sql)
                print(f"✓ {index['description']}创建成功")
                created_count += 1
            except Exception as e:
                # 如果索引已存在但检查失败，记录错误并继续
                if "已经存在" in str(e):
                    print(f"✓ {index['description']}已存在 (创建时检测到)")
                    existing_count += 1
                else:
                    print(f"❌ 创建{index['description']}失败: {str(e)}")
                    # 不抛出异常，继续尝试创建其他索引
        
        # 提交事务
        conn.commit()
        
        # 关闭连接
        cursor.close()
        conn.close()
        
        print(f"✅ 所有索引处理完成 (新创建: {created_count}, 已存在: {existing_count})")
        return {"success": True, "message": f"数据库索引处理完成 (新创建: {created_count}, 已存在: {existing_count})"}
    except Exception as e:
        # 如果出错，回滚事务
        if 'conn' in locals() and conn:
            conn.rollback()
            cursor.close()
            conn.close()
        
        print(f"❌ 创建索引时出错: {str(e)}")
        return {"error": str(e)} 
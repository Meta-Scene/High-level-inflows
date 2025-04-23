from decimal import Decimal
import db_utils
import signal_calculator

def compute_all_stocks_data(force_recompute=False):
    """计算所有股票数据并保存到数据库
    
    参数:
    force_recompute: 是否强制重新计算
    """
    print("\n===== 开始计算最近交易日股票数据 =====")
    
    try:
        # 获取最近的交易日期
        print("正在获取最近的交易日期...")
        latest_dates = db_utils.get_latest_trading_dates()
        if not latest_dates:
            print("❌ 错误: 无法获取最近交易日期")
            return {"error": "无法获取最近交易日期"}
        
        print(f"✓ 获取到最近{len(latest_dates)}个交易日，从 {latest_dates[0]} 到 {latest_dates[-1]}")
        
        # 连接数据库
        print("正在连接数据库...")
        conn = db_utils.get_db_connection()
        cursor = conn.cursor()
        print("✓ 数据库连接成功")
        
        # 获取所有不同的股票代码
        print("正在获取所有股票代码...")
        cursor.execute("SELECT DISTINCT ts_code FROM all_stocks_days")
        all_stocks = [row[0] for row in cursor.fetchall()]
        total_stocks = len(all_stocks)
        print(f"✓ 共找到 {total_stocks} 只股票需要处理")
        
        # 计数器
        signals_count = 0
        processed_count = 0
        
        # 每个股票处理
        print("\n===== 开始处理股票数据 =====")
        for idx, ts_code in enumerate(all_stocks):  # 处理全部股票
            # 显示处理进度
            processed_count += 1
            percentage = processed_count / total_stocks * 100
            # 每处理一只股票都输出一次
            print(f"处理进度: {processed_count}/{total_stocks} ({percentage:.1f}%) - 当前: {ts_code}")
            
            # 只获取最近几个交易日的数据
            cursor.execute("""
                SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, bay, ma120, ma250, name, id
                FROM all_stocks_days
                WHERE ts_code = %s AND trade_date >= %s
                ORDER BY trade_date
            """, (ts_code, latest_dates[-1]))
            
            stock_data = cursor.fetchall()
            if not stock_data:
                print(f"    ⚠️ 股票 {ts_code} 没有最近交易日数据，跳过")
                continue
                
            # 计算买卖点信号
            stock_rows = []
            has_signal = False  # 标记是否有买卖点信号
            
            # 将数据转换为没有ID的格式进行计算
            stock_data_for_calc = [row[:-1] for row in stock_data]
            
            for i in range(0, len(stock_data_for_calc)):
                current_row = stock_data_for_calc[i]
                id_value = stock_data[i][-1]  # 获取ID值
                
                # 转换数据类型，处理Decimal
                stock_row = []
                for j, item in enumerate(current_row):
                    if isinstance(item, Decimal):
                        stock_row.append(float(item))
                    else:
                        stock_row.append(item)
                
                # 计算买入信号和卖出信号
                buy_signal = 0.0
                sell_signal = 0.0
                
                if signal_calculator.calculate_buy_signal(stock_data_for_calc, i):
                    buy_signal = float(stock_row[5])  # 使用当天收盘价作为买点价格
                    has_signal = True  # 有信号
                
                if signal_calculator.calculate_high_fund_outflow(stock_data_for_calc, i):
                    sell_signal = float(stock_row[5])  # 使用当天收盘价作为卖点价格
                    has_signal = True  # 有信号
                
                # 如果有买入或卖出信号，保存到数据库
                if buy_signal > 0 or sell_signal > 0:
                    # 获取all_stocks_days的ID
                    all_stocks_days_id = id_value
                    
                    # 检查是否已经存在记录
                    cursor.execute("""
                        SELECT id FROM high_level_inflows 
                        WHERE all_stocks_days_id = %s
                    """, (all_stocks_days_id,))
                    
                    existing = cursor.fetchone()
                    
                    # 计算收益率
                    earnings_rate = 0.0
                    if sell_signal > 0:
                        # 对于卖出信号，找出后续的最低价计算收益率
                        if i < len(stock_data_for_calc) - 1:
                            current_price = float(stock_data_for_calc[i][5]) if isinstance(stock_data_for_calc[i][5], Decimal) else stock_data_for_calc[i][5]
                            min_price = current_price
                            
                            for j in range(i+1, len(stock_data_for_calc)):
                                price = float(stock_data_for_calc[j][5]) if isinstance(stock_data_for_calc[j][5], Decimal) else stock_data_for_calc[j][5]
                                if price < min_price:
                                    min_price = price
                            
                            if min_price < current_price:
                                earnings_rate = (current_price - min_price) / current_price * 100
                    
                    # 插入或更新记录
                    if existing:
                        # 更新现有记录
                        cursor.execute("""
                            UPDATE high_level_inflows 
                            SET buy = %s, sell = %s, earnings_rate = %s
                            WHERE all_stocks_days_id = %s
                        """, (buy_signal, sell_signal, earnings_rate, all_stocks_days_id))
                    else:
                        # 获取下一个可用的ID
                        cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM high_level_inflows")
                        next_id = cursor.fetchone()[0]
                        
                        # 插入新记录
                        cursor.execute("""
                            INSERT INTO high_level_inflows (id, all_stocks_days_id, buy, sell, earnings_rate)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (next_id, all_stocks_days_id, buy_signal, sell_signal, earnings_rate))
                
                # 构建返回的数据行，保持原始格式，但用我们计算的买卖点替换bay字段
                # ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, buy, ma120, ma250, name, sell
                result_row = stock_row[:9]  # 保留前9个字段
                result_row.append(buy_signal)  # 添加计算得到的买点 (替换bay字段)
                result_row.extend(stock_row[10:13])  # 添加ma120, ma250, name
                result_row.append(sell_signal)  # 添加计算得到的卖点
                
                stock_rows.append(result_row)
            
            # 如果有信号，记录该股票
            if has_signal:
                conn.commit()
                signals_count += 1
                print(f"    ✓ 股票 {ts_code} 有买卖点信号，已保存到数据库")
            else:
                conn.commit()  # 即使没有信号也提交，确保更新了数据库
                print(f"    - 股票 {ts_code} 没有买卖点信号")
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        
        print(f"\n✅ 处理完成！共处理 {total_stocks} 只股票，其中 {signals_count} 只最近{len(latest_dates)}个交易日内有买卖点信号")
        print("\n===== 正在从数据库加载结果 =====")
        
        # 从数据库获取结果
        result = db_utils.get_stocks_with_signals_from_db()
        print(f"✅ 数据加载完成！返回 {result.get('stock_count', 0)} 只股票数据，总共 {result.get('total_stocks', 0)} 只股票")
        
        return result
    except Exception as e:
        print(f"❌ 处理过程中出错: {str(e)}")
        return {"error": str(e)}

def get_all_stocks_data():
    """获取所有股票数据并计算买卖点"""
    print("\n===== 开始获取股票数据 =====")
    # 尝试从数据库获取数据
    print("正在从数据库获取已有数据...")
    result = db_utils.get_stocks_with_signals_from_db()
    
    # 如果没有数据，则重新计算
    if "error" in result:
        print("⚠️ 数据库中没有找到数据，开始重新计算...")
        return compute_all_stocks_data(force_recompute=True)
    
    print(f"✅ 已从数据库成功获取 {result.get('stock_count', 0)} 只股票数据")
    return result 

def get_all_available_stocks_data():
    """获取数据库中所有可用的股票数据，不只是有买卖点信号的"""
    print("\n===== 开始获取所有股票数据 =====")
    
    try:
        # 从数据库获取所有股票的基本信息
        print("正在从数据库获取所有股票信息...")
        result = db_utils.get_all_stocks_info()
        
        if "error" in result:
            print(f"❌ 获取股票信息出错: {result['error']}")
            return result
            
        total_stocks = result.get("total_count", 0)
        print(f"✅ 已找到 {total_stocks} 只股票的基本信息")
        
        return result
    except Exception as e:
        print(f"❌ 处理所有股票数据时出错: {str(e)}")
        return {"error": str(e)} 

def compute_stocks_data_optimized(force_recompute=False, batch_size=100):
    """优化的股票数据计算函数，使用批处理和索引提升性能
    
    参数:
    force_recompute: 是否强制重新计算
    batch_size: 每批处理的股票数量
    """
    print("\n===== 开始优化计算最近交易日股票数据 =====")
    
    try:
        # 获取最近的交易日期
        print("正在获取最近的交易日期...")
        latest_dates = db_utils.get_latest_trading_dates()
        if not latest_dates:
            print("❌ 错误: 无法获取最近交易日期")
            return {"error": "无法获取最近交易日期"}
        
        print(f"✓ 获取到最近{len(latest_dates)}个交易日，从 {latest_dates[0]} 到 {latest_dates[-1]}")
        
        # 连接数据库
        print("正在连接数据库...")
        conn = db_utils.get_db_connection()
        cursor = conn.cursor()
        print("✓ 数据库连接成功")
        
        # 获取所有不同的股票代码
        print("正在获取所有股票代码...")
        cursor.execute("""
            SELECT DISTINCT ts_code 
            FROM all_stocks_days 
            WHERE trade_date >= %s
            ORDER BY ts_code
        """, (latest_dates[-1],))
        
        all_stocks = [row[0] for row in cursor.fetchall()]
        total_stocks = len(all_stocks)
        print(f"✓ 共找到 {total_stocks} 只股票需要处理")
        
        # 计数器
        signals_count = 0
        processed_count = 0
        
        # 批处理股票
        print("\n===== 开始批量处理股票数据 =====")
        
        # 分批处理股票
        for batch_start in range(0, total_stocks, batch_size):
            batch_end = min(batch_start + batch_size, total_stocks)
            batch_stocks = all_stocks[batch_start:batch_end]
            
            print(f"正在处理第 {batch_start//batch_size + 1} 批，股票 {batch_start+1}-{batch_end}/{total_stocks}")
            
            # 为批量查询构建SQL参数
            batch_params = []
            batch_query_parts = []
            
            for ts_code in batch_stocks:
                batch_params.extend([ts_code, latest_dates[-1]])
                batch_query_parts.append("(ts_code = %s AND trade_date >= %s)")
            
            # 只获取最近几个交易日的数据（一次性获取整批股票的数据）
            batch_query = f"""
                SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, bay, ma120, ma250, name, id
                FROM all_stocks_days
                WHERE {" OR ".join(batch_query_parts)}
                ORDER BY ts_code, trade_date
            """
            
            cursor.execute(batch_query, batch_params)
            all_stock_data = cursor.fetchall()
            
            # 按股票代码分组数据
            stocks_data = {}
            for row in all_stock_data:
                ts_code = row[0]
                if ts_code not in stocks_data:
                    stocks_data[ts_code] = []
                stocks_data[ts_code].append(row)
            
            # 处理每只股票
            for ts_code, stock_data in stocks_data.items():
                processed_count += 1
                percentage = processed_count / total_stocks * 100
                print(f"处理进度: {processed_count}/{total_stocks} ({percentage:.1f}%) - 当前: {ts_code}")
                
                if not stock_data:
                    print(f"    ⚠️ 股票 {ts_code} 没有最近交易日数据，跳过")
                    continue
                
                # 计算买卖点信号
                stock_rows = []
                has_signal = False  # 标记是否有买卖点信号
                
                # 将数据转换为没有ID的格式进行计算
                stock_data_for_calc = [row[:-1] for row in stock_data]
                
                # 更新数据库中的信号，并收集结果
                for i in range(0, len(stock_data_for_calc)):
                    current_row = stock_data_for_calc[i]
                    id_value = stock_data[i][-1]  # 获取ID值
                    
                    # 转换数据类型，处理Decimal
                    stock_row = []
                    for j, item in enumerate(current_row):
                        if isinstance(item, Decimal):
                            stock_row.append(float(item))
                        else:
                            stock_row.append(item)
                    
                    # 计算买入信号和卖出信号
                    buy_signal = 0.0
                    sell_signal = 0.0
                    
                    if signal_calculator.calculate_buy_signal(stock_data_for_calc, i):
                        buy_signal = float(stock_row[5])  # 使用当天收盘价作为买点价格
                        has_signal = True  # 有信号
                    
                    if signal_calculator.calculate_high_fund_outflow(stock_data_for_calc, i):
                        sell_signal = float(stock_row[5])  # 使用当天收盘价作为卖点价格
                        has_signal = True  # 有信号
                    
                    # 如果有买入或卖出信号，保存到数据库
                    if buy_signal > 0 or sell_signal > 0:
                        # 获取all_stocks_days的ID
                        all_stocks_days_id = id_value
                        
                        # 检查是否已经存在记录
                        cursor.execute("""
                            SELECT id FROM high_level_inflows 
                            WHERE all_stocks_days_id = %s
                        """, (all_stocks_days_id,))
                        
                        existing = cursor.fetchone()
                        
                        # 计算收益率
                        earnings_rate = 0.0
                        if sell_signal > 0:
                            # 对于卖出信号，找出后续的最低价计算收益率
                            if i < len(stock_data_for_calc) - 1:
                                current_price = float(stock_data_for_calc[i][5]) if isinstance(stock_data_for_calc[i][5], Decimal) else stock_data_for_calc[i][5]
                                min_price = current_price
                                
                                for j in range(i+1, len(stock_data_for_calc)):
                                    price = float(stock_data_for_calc[j][5]) if isinstance(stock_data_for_calc[j][5], Decimal) else stock_data_for_calc[j][5]
                                    if price < min_price:
                                        min_price = price
                                
                                if min_price < current_price:
                                    earnings_rate = (current_price - min_price) / current_price * 100
                        
                        # 插入或更新记录
                        if existing:
                            # 更新现有记录
                            cursor.execute("""
                                UPDATE high_level_inflows 
                                SET buy = %s, sell = %s, earnings_rate = %s
                                WHERE all_stocks_days_id = %s
                            """, (buy_signal, sell_signal, earnings_rate, all_stocks_days_id))
                        else:
                            # 获取下一个可用的ID
                            cursor.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM high_level_inflows")
                            next_id = cursor.fetchone()[0]
                            
                            # 插入新记录
                            cursor.execute("""
                                INSERT INTO high_level_inflows (id, all_stocks_days_id, buy, sell, earnings_rate)
                                VALUES (%s, %s, %s, %s, %s)
                            """, (next_id, all_stocks_days_id, buy_signal, sell_signal, earnings_rate))
                    
                    # 构建返回的数据行，保持原始格式，但用我们计算的买卖点替换bay字段
                    # ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, buy, ma120, ma250, name, sell
                    result_row = stock_row[:9]  # 保留前9个字段
                    result_row.append(buy_signal)  # 添加计算得到的买点 (替换bay字段)
                    result_row.extend(stock_row[10:13])  # 添加ma120, ma250, name
                    result_row.append(sell_signal)  # 添加计算得到的卖点
                    
                    stock_rows.append(result_row)
                
                # 如果有信号，记录该股票
                if has_signal:
                    conn.commit()
                    signals_count += 1
                    print(f"    ✓ 股票 {ts_code} 有买卖点信号，已保存到数据库")
                else:
                    conn.commit()  # 即使没有信号也提交，确保更新了数据库
                    print(f"    - 股票 {ts_code} 没有买卖点信号")
            
            # 每批次结束后提交一次
            conn.commit()
        
        # 关闭数据库连接
        cursor.close()
        conn.close()
        
        print(f"\n✅ 处理完成！共处理 {total_stocks} 只股票，其中 {signals_count} 只最近{len(latest_dates)}个交易日内有买卖点信号")
        print("\n===== 正在从数据库加载结果 =====")
        
        # 从数据库获取结果
        result = db_utils.get_stocks_with_signals_from_db()
        print(f"✅ 数据加载完成！返回 {result.get('stock_count', 0)} 只股票数据，总共 {result.get('total_stocks', 0)} 只股票")
        
        return result
    except Exception as e:
        print(f"❌ 处理过程中出错: {str(e)}")
        return {"error": str(e)} 
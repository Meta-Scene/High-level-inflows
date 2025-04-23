from decimal import Decimal

def calculate_high_fund_outflow(stock_data, i, window=5):
    """计算高位资金净流出信号
    
    参数:
    stock_data: 股票数据列表
    i: 当前数据索引
    window: 计算均线的窗口期
    
    返回:
    bool: 是否存在高位资金净流出信号
    """
    if i < window:
        return False
    
    current_row = stock_data[i]
    
    # 确保有足够的数据计算均线
    prices = []
    volumes = []
    for j in range(i-window+1, i+1):
        if j >= 0 and j < len(stock_data):
            # 收盘价
            if isinstance(stock_data[j][5], Decimal):
                prices.append(float(stock_data[j][5]))
            # 成交量
            if isinstance(stock_data[j][8], Decimal):
                volumes.append(float(stock_data[j][8]))
    
    if len(prices) < window:
        return False
    
    # 计算简单移动平均线
    ma = sum(prices) / len(prices)
    
    # 当前价格
    current_price = float(current_row[5]) if isinstance(current_row[5], Decimal) else 0
    
    # 当前成交量
    current_volume = float(current_row[8]) if isinstance(current_row[8], Decimal) else 0
    
    # 前一日成交量
    prev_volume = 0
    if i > 0:
        prev_row = stock_data[i-1]
        prev_volume = float(prev_row[8]) if isinstance(prev_row[8], Decimal) else 0
    
    # 前一日收盘价
    prev_price = 0
    if i > 0:
        prev_row = stock_data[i-1]
        prev_price = float(prev_row[5]) if isinstance(prev_row[5], Decimal) else 0
    
    # 高位资金净流出条件：
    # 1. 价格处于高位 (高于简单移动平均线)
    # 2. 成交量放大 (比前一天高)
    # 3. 价格下跌 (比前一天低)
    if current_price > ma and current_volume > prev_volume * 1.1 and current_price < prev_price:
        return True
    
    return False

def calculate_buy_signal(stock_data, i, window=5):
    """计算买入信号
    
    参数:
    stock_data: 股票数据列表
    i: 当前数据索引
    window: 计算均线的窗口期
    
    返回:
    bool: 是否存在买入信号
    """
    if i < window:
        return False
    
    current_row = stock_data[i]
    
    # 确保有足够的数据计算均线
    prices = []
    volumes = []
    for j in range(i-window+1, i+1):
        if j >= 0 and j < len(stock_data):
            # 收盘价
            if isinstance(stock_data[j][5], Decimal):
                prices.append(float(stock_data[j][5]))
            # 成交量
            if isinstance(stock_data[j][8], Decimal):
                volumes.append(float(stock_data[j][8]))
    
    if len(prices) < window:
        return False
    
    # 计算简单移动平均线
    ma = sum(prices) / len(prices)
    
    # 当前价格
    current_price = float(current_row[5]) if isinstance(current_row[5], Decimal) else 0
    
    # 当前成交量
    current_volume = float(current_row[8]) if isinstance(current_row[8], Decimal) else 0
    
    # 前一日成交量
    prev_volume = 0
    if i > 0:
        prev_row = stock_data[i-1]
        prev_volume = float(prev_row[8]) if isinstance(prev_row[8], Decimal) else 0
    
    # 前一日收盘价
    prev_price = 0
    if i > 0:
        prev_row = stock_data[i-1]
        prev_price = float(prev_row[5]) if isinstance(prev_row[5], Decimal) else 0
    
    # 买入信号条件：
    # 1. 价格处于低位 (低于简单移动平均线)
    # 2. 成交量放大 (比前一天高)
    # 3. 价格上涨 (比前一天高)
    if current_price < ma * 0.95 and current_volume > prev_volume * 1.1 and current_price > prev_price:
        return True
    
    return False

def calculate_return_rate(stock_data):
    """计算股票的收益率
    
    参数:
    stock_data: 股票数据列表，每行包含股票信息和买卖点信号
    
    返回:
    dict: 包含收益率信息的字典
    """
    if not stock_data or len(stock_data) == 0:
        return {
            "has_signals": False,
            "return_rate": 0.0,
            "signals": []
        }
    
    signals = []
    
    # stock_data中每个元素的结构：
    # [ts_code, trade_date, open, high, low, close, pre_close, pct_chg, vol, buy, ma120, ma250, name, sell]
    
    # 找出所有卖出信号
    for i, row in enumerate(stock_data):
        if row[-1] > 0:  # 最后一个元素是卖出信号
            date = row[1]  # trade_date
            price = row[5]  # close
            signals.append({
                "date": date,
                "price": price,
                "type": "sell"
            })
    
    # 如果没有信号，直接返回
    if not signals:
        return {
            "has_signals": False,
            "return_rate": 0.0,
            "signals": []
        }
    
    # 对于每个卖出信号，找出之后的最低价来计算收益率
    return_rates = []
    for i, signal in enumerate(signals):
        # 查找信号后的数据
        signal_index = next((j for j, row in enumerate(stock_data) if row[1] == signal["date"]), -1)
        
        if signal_index == -1 or signal_index >= len(stock_data) - 1:
            # 如果是最后一天的信号，无法计算收益率
            continue
        
        # 查找信号后的最低价
        min_price = float('inf')
        min_date = None
        for j in range(signal_index + 1, len(stock_data)):
            current_price = stock_data[j][5]  # close
            if current_price < min_price:
                min_price = current_price
                min_date = stock_data[j][1]  # trade_date
        
        # 如果找到了最低价，计算收益率
        if min_price < float('inf'):
            sell_price = signal["price"]
            return_rate = (sell_price - min_price) / sell_price * 100
            
            return_rates.append({
                "sell_date": signal["date"],
                "sell_price": sell_price,
                "min_date": min_date,
                "min_price": min_price,
                "return_rate": return_rate
            })
    
    # 计算平均收益率
    if return_rates:
        avg_return_rate = sum(r["return_rate"] for r in return_rates) / len(return_rates)
    else:
        avg_return_rate = 0.0
    
    return {
        "has_signals": True,
        "return_rate": avg_return_rate,
        "signals": len(signals),
        "return_details": return_rates
    } 
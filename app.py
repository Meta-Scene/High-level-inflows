from flask import Flask, jsonify, request
import json
from decimal import Decimal
import db_utils
import data_processor

app = Flask(__name__)

# 自定义JSON编码器，处理Decimal类型
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

@app.route('/')
def index():
    """API根路径，显示可用端点"""
    return '''
    <h1>股票API</h1>
    <p>可用的API端点:</p>
    <ul>
        <li><a href="/api/stocks">/api/stocks</a> - 获取有买卖点信号的股票数据（最近20个交易日内）</li>
        <li>/api/stocks/股票代码 - 获取单只股票数据 (例如: <a href="/api/stocks/000001.SZ">/api/stocks/000001.SZ</a>)</li>
        <li><a href="/api/refresh">/api/refresh</a> - 强制刷新计算结果
            <ul>
                <li>可选参数:
                    <ul>
                        <li><code>optimized=true|false</code> - 是否使用优化计算（默认为true）</li>
                        <li><code>batch_size=整数</code> - 批处理大小（默认为100）</li>
                    </ul>
                </li>
                <li>例如: <a href="/api/refresh?batch_size=50">/api/refresh?batch_size=50</a> - 使用批大小为50的优化计算</li>
                <li>例如: <a href="/api/refresh?optimized=false">/api/refresh?optimized=false</a> - 使用常规计算</li>
            </ul>
        </li>
        <li><a href="/api/returns">/api/returns</a> - 获取所有股票的收益率统计</li>
        <li><a href="/api/all-stocks">/api/all-stocks</a> - 获取数据库中所有股票的完整列表（不只限于有信号的股票）</li>
        <li><a href="/api/index">/api/index</a> - 创建或更新数据库索引以提升查询性能</li>
    </ul>
    
    <h2>性能优化说明</h2>
    <p>本API已进行以下优化以提高性能：</p>
    <ol>
        <li>为关键字段添加数据库索引（ts_code、trade_date等）</li>
        <li>使用批处理优化股票数据计算</li>
        <li>使用更高效的SQL查询减少数据库负载</li>
    </ol>
    <p>使用<a href="/api/index">/api/index</a>端点可以创建或更新数据库索引，在大量数据的情况下这会显著提升查询速度。</p>
    '''

@app.route('/api/stocks')
def all_stocks():
    """返回所有股票数据"""
    result = data_processor.get_all_stocks_data()
    return json.dumps(result, ensure_ascii=False, cls=CustomJSONEncoder), 200, {'Content-Type': 'application/json; charset=utf-8'}

@app.route('/api/stocks/<string:ts_code>')
def single_stock(ts_code):
    """返回单只股票数据"""
    result = db_utils.get_single_stock_data(ts_code)
    return json.dumps(result, ensure_ascii=False, cls=CustomJSONEncoder), 200, {'Content-Type': 'application/json; charset=utf-8'}

@app.route('/api/returns')
def stock_returns():
    """返回所有股票的收益率统计"""
    result = data_processor.get_all_stocks_data()
    if "stock_returns" in result:
        return json.dumps({"stock_returns": result["stock_returns"]}, ensure_ascii=False, cls=CustomJSONEncoder), 200, {'Content-Type': 'application/json; charset=utf-8'}
    else:
        return json.dumps({"error": "没有找到收益率数据"}, ensure_ascii=False), 200, {'Content-Type': 'application/json; charset=utf-8'}

@app.route('/api/all-stocks')
def get_all_stocks_list():
    """获取数据库中所有股票的列表，不仅仅是有信号的股票"""
    try:
        result = data_processor.get_all_available_stocks_data()
        if "error" in result:
            return json.dumps({"error": result["error"]}, ensure_ascii=False), 200, {'Content-Type': 'application/json; charset=utf-8'}
        
        return json.dumps(result, ensure_ascii=False, cls=CustomJSONEncoder), 200, {'Content-Type': 'application/json; charset=utf-8'}
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False), 500, {'Content-Type': 'application/json; charset=utf-8'}

@app.route('/api/refresh')
def refresh_data():
    """强制刷新数据"""
    try:
        print("\n===== 强制刷新数据开始 =====")
        
        # 获取批处理大小参数
        batch_size = request.args.get('batch_size', default=100, type=int)
        
        # 获取是否使用优化计算的参数
        use_optimized = request.args.get('optimized', default='true', type=str).lower() == 'true'
        
        # 根据参数选择使用哪个计算函数
        if use_optimized:
            print(f"使用优化计算函数，批处理大小: {batch_size}")
            result = data_processor.compute_stocks_data_optimized(force_recompute=True, batch_size=batch_size)
        else:
            print("使用常规计算函数")
            result = data_processor.compute_all_stocks_data(force_recompute=True)
        
        print("===== 强制刷新数据完成 =====\n")
        
        return json.dumps({
            "success": True, 
            "message": f"数据已刷新 (使用{'优化' if use_optimized else '常规'}方法)",
            "stock_count": result.get("stock_count", 0),
            "total_stocks": result.get("total_stocks", 0),
            "batch_size": batch_size if use_optimized else "N/A"
        }, ensure_ascii=False, cls=CustomJSONEncoder), 200, {'Content-Type': 'application/json; charset=utf-8'}
    except Exception as e:
        print(f"❌ 刷新数据时出错: {str(e)}")
        return json.dumps({"error": str(e)}, ensure_ascii=False), 500, {'Content-Type': 'application/json; charset=utf-8'}

@app.route('/api/index')
def create_index():
    """创建数据库索引以提升查询性能"""
    print("\n===== 开始创建数据库索引 =====")
    result = db_utils.create_database_indexes()
    print("===== 索引创建完成 =====\n")
    return json.dumps(result, ensure_ascii=False, cls=CustomJSONEncoder), 200, {'Content-Type': 'application/json; charset=utf-8'}

# 添加简单路由，重定向到API路径
@app.route('/stocks')
def stocks_redirect():
    return all_stocks()

@app.route('/stocks/<string:ts_code>')
def stock_redirect(ts_code):
    return single_stock(ts_code)

@app.errorhandler(404)
def page_not_found(e):
    """处理404错误"""
    return '''
    <h1>页面未找到</h1>
    <p>您访问的URL不存在。请尝试以下链接：</p>
    <ul>
        <li><a href="/">主页</a></li>
        <li><a href="/api/stocks">有信号的股票数据</a></li>
        <li><a href="/api/stocks/000001.SZ">平安银行(000001.SZ)数据</a></li>
        <li><a href="/api/returns">所有股票收益率</a></li>
        <li><a href="/api/all-stocks">所有股票完整列表</a></li>
        <li><a href="/api/index">创建/更新数据库索引</a></li>
    </ul>
    ''', 404

if __name__ == '__main__':
    # 启动时尝试确保计算一次
    try:
        print("\n===== 服务启动，正在预加载数据... =====")

        # 先创建数据库索引，提升后续查询速度
        print("\n===== 正在创建/检查数据库索引... =====")
        db_utils.create_database_indexes()
        
        # 检查是否已有数据
        print("正在检查数据库中是否已有数据...")
        result = db_utils.get_stocks_with_signals_from_db()
        if "error" in result:
            # 如果没有数据，则使用优化方式重新计算
            print("⚠️ 数据库中没有找到数据，开始首次计算...")
            batch_size = 100  # 默认批处理大小
            print(f"使用优化计算函数，批处理大小: {batch_size}")
            data_processor.compute_stocks_data_optimized(force_recompute=True, batch_size=batch_size)
        else:
            print(f"✅ 已找到 {result.get('stock_count', 0)} 只股票的数据，无需重新计算")
        
        print("\n===== 数据预加载完成，服务准备就绪 =====")
        print("API服务启动，可通过浏览器访问 http://0.0.0.0:5000/")
    except Exception as e:
        print(f"❌ 预加载数据时出错: {str(e)}")
    
    app.run(host='0.0.0.0', port=5000, debug=True) 
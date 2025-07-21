import threading
import pandas as pd
import numpy as np
from datetime import datetime
from LZCTrader.strategy import Strategy
from brokers.broker import Broker
from LZCTrader.order import Order


class TrendFollow(Strategy):
    """拐点趋势跟随策略 (Reversal Following Strategy)

    策略规则:
    ------
    1. 使用滑动窗口极值滤波识别拐点
    2. 结合成交量变化确认拐点有效性
    3. 上拐点生成做多信号，下拐点生成做空信号（顺势突破）
    4. 设置最小拐点间隔避免过度交易
    5. 动态仓位管理
    """

    def __init__(
        self, instrument: str, exchange: str, parameters: dict, broker: Broker  # 这四个是必需参数
    ) -> None:
        # 必需：
        self.instrument = instrument  # 品种
        self.exchange = exchange  # 交易所
        self.params = parameters  # 策略参数
        self.broker = broker  # 功能接口

        # 自定义：
        self.trade_num = self.params.get('trade_num', 1)  # 交易手数
        self.trade_offset = self.params.get('trade_offset', 3)  # 取买几卖几
        self.lock = threading.Lock()  # 线程锁

        # 拐点检测参数
        self.window_size = self.params.get("window_size", 5)  # 滑动窗口大小（分钟）
        self.volume_threshold = self.params.get("volume_threshold", 1.5)  # 成交量阈值
        self.min_interval = self.params.get("min_interval", 5)  # 最小拐点间隔（分钟）
        
        # 回测模式参数
        self.backtest_mode = self.params.get("backtest_mode", False)  # 是否回测模式
        
        # 拐点历史记录
        self.last_signal_time = None
        self.last_data_time = None  # 记录上次数据时间

    def min_generate_features(self, data: pd.DataFrame):
        # 在此函数中，根据传入参数data，计算出你策略所需的指标，非必需
        # 这里可以添加技术指标计算，如MA、EMA等
        return data

    def generate_signal(self, dt: datetime):
        # 此为函数主体，根据指标进行计算，产生交易信号并下单，程序只会调用这一个函数进行不断循环。必需

        new_orders = []
        data = self.broker.get_candles(self.instrument, granularity="1min", count=30, cut_yesterday=True)  # 取行情数据函数示例
        # granularity：时间粒度，支持1s，5s，1min，1h等；
        # count：取k线的数目；
        # cut_yesterday：取的数据中，当同时包含今日数据和昨日数据时，是否去掉昨日数据。True表示去掉；
        data = data[::-1]  # 取到的数据中，按时间由近到远排序。再此翻转为由远到近，便于某些策略处理

        position_dict = self.broker.get_position(self.instrument)
        print(f"{self.instrument} position", position_dict["long_tdPosition"], position_dict["long_ydPosition"], position_dict["short_tdPosition"], position_dict["short_ydPosition"]) # 仓位查询

        # 检查数据是否足够进行拐点检测
        if len(data) < self.window_size * 2:
            print(f"{self.instrument}: 数据不足，跳过拐点检测")
            return new_orders

        # 检查数据时间戳是否与上次相同，避免处理重复数据
        if self.last_data_time is not None and data.index[0] == self.last_data_time:
            print(f"{self.instrument}: 数据时间戳与上次相同，跳过拐点检测")
            return new_orders
        
        # 检查数据时间是否合理（不能太旧）
        current_time = datetime.now()
        latest_data_time = data.index[-1]  # 最新数据时间
        
        # 如果数据时间比当前时间早超过1小时，认为是过期数据
        time_diff = current_time - latest_data_time
        if time_diff.total_seconds() > 3600:  # 1小时 = 3600秒
            print(f"{self.instrument}: 数据时间过旧 ({latest_data_time})，跳过处理")
            print(f"{self.instrument}: 当前时间: {current_time}, 时间差: {time_diff}")
            
            if self.backtest_mode:
                print(f"{self.instrument}: 回测模式 - 继续分析历史数据")
                # 回测模式下继续处理历史数据
            else:
                print(f"{self.instrument}: 实盘/模拟盘模式 - 跳过过期数据")
                return new_orders
        
        self.last_data_time = data.index[0]

        # 检测最新数据点是否为拐点 - 修复索引逻辑
        latest_index = len(data) - 1  # 直接使用最新数据
        if latest_index < self.window_size:
            print(f"{self.instrument}: 最新索引小于窗口大小，跳过")
            return new_orders

        # 获取滑动窗口数据（以最新数据为中心）
        start_idx = max(0, latest_index - self.window_size)
        end_idx = min(len(data), latest_index + self.window_size + 1)
        window_data = data.iloc[start_idx:end_idx]
        
        current_price = data.iloc[latest_index]['Close']
        current_volume = data.iloc[latest_index]['Volume']
        window_high = window_data['High'].max()
        window_low = window_data['Low'].min()
        window_volume_mean = window_data['Volume'].mean()
        current_time = data.index[latest_index]

        # 添加调试输出
        print(f"{self.instrument}: 价格={current_price:.2f}, 成交量={current_volume:.0f}")
        print(f"{self.instrument}: 窗口最高={window_high:.2f}, 最低={window_low:.2f}, 平均成交量={window_volume_mean:.0f}")

        # 信号强度法动态仓位
        dynamic_volume = self.get_dynamic_volume(current_volume, window_volume_mean)
        print(f"{self.instrument}: 动态下单手数={dynamic_volume}")

        # 检测上拐点（局部高点）或价格突破
        price_breakout = current_price > window_high * 0.999  # 允许0.1%的误差
        volume_surge = current_volume > window_volume_mean * self.volume_threshold
        
        if ((current_price >= window_high or price_breakout) and 
            volume_surge and
            self._check_min_interval(current_time)):
            
            print(f"{self.instrument}: 检测到上拐点信号！")
            self.broker.relog()  # 由于一段时间不登录，交易所可能会自动下线，所以每次下单前先登录
            duo_enter_point = current_price + self.trade_offset  # 下单价。为保证立刻成交，在此取买三、卖三报单，按照价格优先原则，会按当前价成交。取买几、卖几可自定义。
            new_order = Order(
                instrument=self.instrument,
                exchange=self.exchange,
                direction=2,  # 2为买，3为卖
                offset=1,  # 1为开仓，4为平今，5为平昨
                price=duo_enter_point,  # 下单价
                volume=dynamic_volume,  # 用动态手数
                stopPrice=0,  # 未实现功能。设为0即可
                orderPriceType=1  # 类型：限价单（现在限价单和市价单由报单价决定。以开多仓为例，报单价比当前价高，则立即成交，相当于市价单。报单价比当前价低，则需等价格跌到此价才成交，相当于现价单）
            )
            new_orders.append(new_order)
            self.write_order(type=1, point=current_price)  # 记录下单结果
            self.last_signal_time = current_time

        # 检测下拐点（局部低点）或价格突破
        elif ((current_price <= window_low or current_price < window_low * 1.001) and 
              volume_surge and
              self._check_min_interval(current_time)):
            
            print(f"{self.instrument}: 检测到下拐点信号！")
            self.broker.relog()
            kong_enter_point = current_price - self.trade_offset
            new_order = Order(
                instrument=self.instrument,
                exchange=self.exchange,
                direction=3,
                offset=1,
                price=kong_enter_point,
                volume=dynamic_volume,  # 用动态手数
                stopPrice=0,
                orderPriceType=1
            )
            new_orders.append(new_order)
            self.write_order(type=3, point=current_price)
            self.last_signal_time = current_time
        else:
            print(f"{self.instrument}: 未检测到拐点信号")

        return new_orders
    
    def get_dynamic_volume(self, current_volume, window_volume_mean):
        """信号强度法动态仓位管理：成交量/均值，最少1手，最多5手"""
        base_volume = self.trade_num  # 配置文件基础手数
        strength = current_volume / window_volume_mean if window_volume_mean > 0 else 1
        volume = int(base_volume * strength)
        return max(1, min(volume, 5))  # 限定最大5手

    def _check_min_interval(self, current_time) -> bool:
        """检查是否满足最小拐点间隔约束"""
        if self.last_signal_time is None:
            return True
        
        # 处理时间差计算
        if hasattr(current_time, 'to_pydatetime'):
            current_time = current_time.to_pydatetime()
        if hasattr(self.last_signal_time, 'to_pydatetime'):
            last_time = self.last_signal_time.to_pydatetime()
        else:
            last_time = self.last_signal_time
            
        if isinstance(current_time, datetime) and isinstance(last_time, datetime):
            time_diff = (current_time - last_time).total_seconds() / 60
        else:
            # 如果时间不是datetime对象，假设是索引位置，简单判断
            time_diff = 10  # 默认满足间隔要求
        
        return time_diff >= self.min_interval

    def write_order(self, type, point):  # 记录下单结果函数，非必需
        now = datetime.now().strftime("%m-%d %H:%M:%S")
        if type == 1:  # 买开
            line = f"{now} {self.instrument}，买开，{point} \n"
        elif type == 2:  # 买平
            line = f"{now} {self.instrument}，买平，{point} \n"
        elif type == 3:  # 卖开
            line = f"{now} {self.instrument}，卖开，{point} \n"
        elif type == 4:  # 卖平
            line = f"{now} {self.instrument}，卖平，{point} \n"
        else:
            raise ValueError("Invalid type")

        with self.lock:
            with open("result/order_book.txt", "a", encoding="utf-8") as f:
                f.write(line) 
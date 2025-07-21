#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API数据检查工具
专门用于检查API返回的数据是否正确
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from API import Context
from brokers.futures import Futures

class APIDataChecker:
    def __init__(self):
        # 配置参数
        self.license_key = 's3az29vbx5w3'
        self.fc_code = 'simnow'
        self.user_id = '244429'
        self.password = 'Axe0910Yao!'
        
        self.api_context = None
        self.futures_broker = None
        
    def connect(self):
        """连接API"""
        print("正在连接API...")
        try:
            self.api_context = Context(
                lisence=self.license_key, 
                fc_code=self.fc_code, 
                user_id=self.user_id, 
                password=self.password
            )
            print("API连接成功!")
            
            self.futures_broker = Futures(
                enter_license=self.license_key, 
                fc_code=self.fc_code, 
                account=self.user_id, 
                password=self.password
            )
            print("期货接口创建成功!")
            return True
            
        except Exception as e:
            print(f"连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开连接"""
        if self.api_context:
            self.api_context.stop()
            print("API连接已断开")
    
    def check_data_timestamps(self, instrument, count=10):
        """检查数据时间戳"""
        print(f"\n检查品种 {instrument} 的数据时间戳:")
        print("-" * 50)
        
        try:
            # 获取数据
            data = self.futures_broker.get_candles(
                instrument=instrument,
                granularity="1min",
                count=count,
                cut_yesterday=False
            )
            
            if data is None or len(data) == 0:
                print("未获取到数据")
                return False
            
            print(f"获取到 {len(data)} 条数据")
            print(f"数据时间范围: {data.index[0]} 到 {data.index[-1]}")
            
            # 检查时间戳
            current_time = datetime.now()
            latest_data_time = data.index[-1]
            earliest_data_time = data.index[0]
            
            print(f"当前时间: {current_time}")
            print(f"最新数据时间: {latest_data_time}")
            print(f"最早数据时间: {earliest_data_time}")
            
            # 计算时间差
            latest_diff = current_time - latest_data_time
            earliest_diff = current_time - earliest_data_time
            
            print(f"最新数据时间差: {latest_diff}")
            print(f"最早数据时间差: {earliest_diff}")
            
            # 判断数据质量
            if latest_diff.total_seconds() > 3600:  # 1小时
                print("❌ 数据过旧 - 可能是历史数据")
                return False
            elif latest_diff.total_seconds() > 300:  # 5分钟
                print("⚠️  数据较旧 - 可能有延迟")
            else:
                print("✅ 数据时间正常")
            
            # 检查数据连续性
            time_diffs = []
            for i in range(1, len(data)):
                diff = data.index[i] - data.index[i-1]
                time_diffs.append(diff.total_seconds())
            
            if time_diffs:
                avg_diff = sum(time_diffs) / len(time_diffs)
                print(f"数据间隔: 平均 {avg_diff:.0f} 秒")
                
                if abs(avg_diff - 60) > 10:  # 允许10秒误差
                    print("⚠️  数据间隔异常")
                else:
                    print("✅ 数据间隔正常")
            
            return True
            
        except Exception as e:
            print(f"检查数据时出错: {e}")
            return False
    
    def check_data_values(self, instrument, count=10):
        """检查数据值"""
        print(f"\n检查品种 {instrument} 的数据值:")
        print("-" * 50)
        
        try:
            data = self.futures_broker.get_candles(
                instrument=instrument,
                granularity="1min",
                count=count,
                cut_yesterday=False
            )
            
            if data is None or len(data) == 0:
                print("未获取到数据")
                return False
            
            print(f"数据列: {list(data.columns)}")
            print(f"数据形状: {data.shape}")
            
            # 检查最新数据
            latest = data.iloc[-1]
            print(f"\n最新数据:")
            print(f"  开盘价: {latest['Open']}")
            print(f"  最高价: {latest['High']}")
            print(f"  最低价: {latest['Low']}")
            print(f"  收盘价: {latest['Close']}")
            print(f"  成交量: {latest['Volume']}")
            
            # 检查数据合理性
            issues = []
            
            # 检查价格合理性
            if latest['High'] < latest['Low']:
                issues.append("最高价小于最低价")
            if latest['Open'] < 0 or latest['Close'] < 0:
                issues.append("价格出现负值")
            
            # 检查成交量合理性
            if latest['Volume'] < 0:
                issues.append("成交量为负值")
            
            # 检查价格变化
            if len(data) > 1:
                prev_close = data.iloc[-2]['Close']
                price_change = latest['Close'] - prev_close
                change_pct = abs(price_change / prev_close) * 100
                
                print(f"价格变化: {price_change:.2f} ({change_pct:.2f}%)")
                
                if change_pct > 10:  # 超过10%的变化
                    issues.append(f"价格变化过大: {change_pct:.2f}%")
            
            if issues:
                print("❌ 发现数据问题:")
                for issue in issues:
                    print(f"  - {issue}")
                return False
            else:
                print("✅ 数据值正常")
                return True
                
        except Exception as e:
            print(f"检查数据值时出错: {e}")
            return False
    
    def check_data_consistency(self, instrument, count=10):
        """检查数据一致性"""
        print(f"\n检查品种 {instrument} 的数据一致性:")
        print("-" * 50)
        
        try:
            # 连续获取两次数据
            data1 = self.futures_broker.get_candles(
                instrument=instrument,
                granularity="1min",
                count=count,
                cut_yesterday=False
            )
            
            time.sleep(2)  # 等待2秒
            
            data2 = self.futures_broker.get_candles(
                instrument=instrument,
                granularity="1min",
                count=count,
                cut_yesterday=False
            )
            
            if data1 is None or data2 is None:
                print("获取数据失败")
                return False
            
            print(f"第一次获取: {len(data1)} 条数据")
            print(f"第二次获取: {len(data2)} 条数据")
            
            # 检查时间戳是否更新
            if data1.index[-1] == data2.index[-1]:
                print("❌ 数据时间戳没有更新")
                return False
            else:
                print("✅ 数据时间戳已更新")
                print(f"  第一次最新时间: {data1.index[-1]}")
                print(f"  第二次最新时间: {data2.index[-1]}")
            
            # 检查价格是否变化
            if data1.iloc[-1]['Close'] == data2.iloc[-1]['Close']:
                print("⚠️  最新价格没有变化")
            else:
                print("✅ 价格有变化")
                print(f"  第一次价格: {data1.iloc[-1]['Close']}")
                print(f"  第二次价格: {data2.iloc[-1]['Close']}")
            
            return True
            
        except Exception as e:
            print(f"检查数据一致性时出错: {e}")
            return False
    
    def run_full_check(self, instruments=None):
        """运行完整检查"""
        if instruments is None:
            instruments = ['ag2508', 'ao2509', 'rb2510', 'jd2508']
        
        print("开始API数据完整性检查")
        print("=" * 60)
        print(f"检查时间: {datetime.now()}")
        print(f"检查品种: {instruments}")
        
        if not self.connect():
            return
        
        try:
            results = {}
            
            for instrument in instruments:
                print(f"\n{'='*20} 检查 {instrument} {'='*20}")
                
                # 检查时间戳
                timestamp_ok = self.check_data_timestamps(instrument)
                
                # 检查数据值
                values_ok = self.check_data_values(instrument)
                
                # 检查一致性
                consistency_ok = self.check_data_consistency(instrument)
                
                # 汇总结果
                results[instrument] = {
                    'timestamp': timestamp_ok,
                    'values': values_ok,
                    'consistency': consistency_ok,
                    'overall': timestamp_ok and values_ok and consistency_ok
                }
                
                print(f"\n{instrument} 检查结果:")
                print(f"  时间戳: {'✅' if timestamp_ok else '❌'}")
                print(f"  数据值: {'✅' if values_ok else '❌'}")
                print(f"  一致性: {'✅' if consistency_ok else '❌'}")
                print(f"  总体: {'✅' if results[instrument]['overall'] else '❌'}")
                
                time.sleep(1)  # 避免请求过于频繁
            
            # 输出总结
            print(f"\n{'='*60}")
            print("检查总结:")
            print("=" * 60)
            
            for instrument, result in results.items():
                status = "✅ 正常" if result['overall'] else "❌ 异常"
                print(f"{instrument}: {status}")
            
            all_ok = all(result['overall'] for result in results.values())
            print(f"\n总体状态: {'✅ 所有数据正常' if all_ok else '❌ 存在数据问题'}")
            
        finally:
            self.disconnect()

def main():
    """主函数"""
    checker = APIDataChecker()
    
    # 运行完整检查
    checker.run_full_check()
    
    # 或者单独检查某个品种
    # checker.connect()
    # checker.check_data_timestamps('ag2508')
    # checker.disconnect()

if __name__ == "__main__":
    main() 
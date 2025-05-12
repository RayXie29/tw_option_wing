import os
import time
import datetime
import operator
import numpy as np
from functools import reduce
from dotenv import load_dotenv
from collections import defaultdict
from typing import Dict, List, Callable, Optional

import shioaji as sj
from shioaji import TickFOPv1, Exchange
from shioaji.constant import Action, StockPriceType, OrderType

from msg import send_to_telegram

load_dotenv()


def timestamp_2_time(ts):
    return datetime.datetime.fromtimestamp(ts / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')


BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]

ops = {
        ">": operator.gt,
        "<": operator.lt,
        "==": operator.eq,
        ">=": operator.ge,
        "<=": operator.le,
        "!=": operator.ne
}

class market:

    def __init__(self):

        self.exchange=None
        self.close=None
    
    def update(self, exchange, tick):
        self.exchange = exchange
        self.close = int(tick.close)

def quote_callback(exchange:Exchange, tick:TickFOPv1):
    mkt.update(exchange, tick)    
def order_callback(stat, msg):
    coh.handle_message(msg)

def get_bear_call_spread(price, calls):
    p1,p2=None, None
    for i in range(len(calls)):
        call = calls[i]
        if i>0:
            prev_call = calls[i-1]
        else:
            prev_call = None
        
        if prev_call is not None and price < call and price > prev_call:
            p1 = call
            if i+1 < len(calls):
                p2 = calls[i+1]
            else:
                p2 = None
            break
    return p1, p2

def get_bull_put_spread(price, puts):
    p1, p2 = None, None
    for i in range(len(puts)):
        put = puts[i]
        if i>0:
            prev_put = puts[i-1]
        else:
            prev_put = None
        if prev_put is not None and price < put and price >prev_put:
            p1 = prev_put
            if i-2 >= 0:
                p2 = puts[i-2]
            else:
                p2 = None
            break
    return p1, p2


def calculate_ranges(open_price, std_val):

    p5 = std_val*0.5

    first_bounds = [open_price+p5, open_price-p5]
    second_bounds = [first_bounds[0]+p5, first_bounds[-1]-p5]
    third_bounds = [second_bounds[0]+std_val, second_bounds[-1]-std_val]
    forth_bounds = [third_bounds[0]+std_val, third_bounds[-1]-std_val]

    total_bounds = np.concatenate([first_bounds, second_bounds, third_bounds, forth_bounds])
    return sorted(total_bounds)

def get_options(contract):
    options = defaultdict(dict)
    options['C'] = {}
    options['P'] = {}


    for key in contract.keys():
        cname = key[:3]
        dmonth = key[3:9]
        price = key[9:-1]
        side = key[-1]
        options[side][int(price)] = key
    return options

class combo_order:

    def __init__(self, contract, p, side=0, option_prices=[], option_dict={}):
        #side
        #   0 : call
        #   1 : put

        side_dict = {
            0 : 'C',
            1 : 'P'
        }
        self.contract = contract
        self.p=p
        self.side=side
        self.trigger=False
        self.stop=False
        
        if side == 0:
            self.l1p, self.l2p = get_bear_call_spread(self.p, option_prices)
            c1_name = option_dict[side_dict[self.side]][self.l1p]
            c2_name = option_dict[side_dict[self.side]][self.l2p]
            
            self.c1 = getattr(self.contract, c1_name)
            self.c2 = getattr(self.contract, c2_name)
        elif side == 1:
            self.l1p, self.l2p = get_bull_put_spread(self.p, option_prices)
            c1_name = option_dict[side_dict[self.side]][self.l1p]
            c2_name = option_dict[side_dict[self.side]][self.l2p]
            
            self.c1 = getattr(self.contract, c1_name)
            self.c2 = getattr(self.contract, c2_name)

def calculate_order_prices(combo_orders):
    open_base_pt = 22
    close_base_pt = 38
    base_pd = 50
    keys = list(combo_orders.keys())
    for key in keys:
        order = combo_orders[key]['enter_order']
        
        order_pt = abs(int(order.c1.symbol[9:-1]) - int(order.c2.symbol[9:-1]))
        cur_open_pt = open_base_pt * order_pt / base_pd
        cur_close_pt = close_base_pt * order_pt / base_pd
        combo_orders[key]['open_price'] = cur_open_pt
        combo_orders[key]['close_price'] = cur_close_pt
    return combo_orders


def placing_order(api, order, p, q, order_type='open'):
    leg1 = order.c1
    leg2 = order.c2


    leg1_act = 'Sell' if order_type == 'open' else 'Buy'
    leg2_act = 'Buy' if order_type == 'open' else 'Sell'
    otype = sj.constant.FuturesOCType.New if order_type == 'open' else sj.constant.FuturesOCType.Cover
    

    combo_contract = sj.contracts.ComboContract(
        legs=[
            sj.contracts.ComboBase(action=leg1_act, **leg1.dict()),
            sj.contracts.ComboBase(action=leg2_act, **leg2.dict()),
        ]
    )
    
    order = api.ComboOrder(
        price_type="LMT", 
        price=p, 
        quantity=q, 
        order_type="IOC",
        octype=otype,
    )

    trade = api.place_comboorder(combo_contract, order)
    
class ComboOrderHandler:
    """
    單筆組合期權 IOC 訂單狀態追蹤器（僅處理同一筆訂單）。

    使用流程：
      1. api.set_order_callback(order_callback)，在回呼中呼叫 handler.handle_message(msg)
      2. 發單後等待一段時間（例如 1-2 秒）
      3. 呼叫 handler.evaluate() 取得最終結果，並重置內部訊息緩衝

    方法
    -------
    handle_message(msg: Dict) -> None
        累積接收到的回覆訊息（自動適用相同 order_id）

    evaluate() -> Dict
        計算並回傳最終統計結果，包含三種可能狀態：
        'filled'   - 全部成交
        'cancelled'- 全部取消
        'partial'  - 部分成交

    evaluate 返回欄位
    ----------------
    order_id     : str    訂單編號
    status       : str    訂單最終狀態 ('filled'/'cancelled'/'partial')
    original_qty : int    原始委託總量（New 訊息之總和）
    filled_qty   : int    成交量（trade 訊息之總和）
    cancel_qty   : int    取消量（Cancel 訊息之 cancel_quantity 總和）
    unfilled_qty : int    未成交未取消量
    msgs         : List[Dict]  計算過程使用的所有訊息
    """

    def __init__(self):
        # 緩衝所有回覆訊息
        self._msgs: List[Dict] = []
        self.status = None
        self.left_q = 0
    def handle_message(self, msg: Dict) -> None:
        """
        處理並儲存單筆交易所回覆訊息。
        """
        self._msgs.append(msg)
    def evaluate(self) -> Dict:
        """
        計算所有累積訊息後的最終訂單狀態，並清空緩衝。
        """
        msgs = self._msgs
        # 清空緩衝
        self._msgs = []

        if not msgs:
            raise ValueError("No messages to evaluate.")

        # 擷取訂單編號
        first = msgs[0]
        if 'operation' in first:
            order_id = first['order']['id']
        else:
            order_id = first.get('trade_id', '')

        original_qty = 0
        filled_qty = 0
        cancel_qty = 0

        for m in msgs:
            if 'operation' in m:
                op = m['operation'].get('op_type')
                if op == 'New':
                    # 僅從 New 訊息累加原始數量
                    original_qty += m['order'].get('quantity', 0)
                elif op == 'Cancel':
                    # 從 Cancel 訊息累加取消數量
                    cancel_qty += m.get('status', {}).get('cancel_quantity', 0)
            else:
                # trade 訊息
                filled_qty += m.get('quantity', 0)

        # 計算未成交未取消量
        self.left_q = (original_qty - filled_qty)//2

        # 判斷狀態
        if original_qty > 0 and filled_qty == original_qty:
            self.status = 'filled'
        elif original_qty > 0 and cancel_qty == original_qty:
            self.status = 'cancelled'
        else:
            self.status = 'partial'


def seconds_until_next_open(now=None):
    """
    Return how many seconds from `now` until the next time
    the market exits its closed windows:
      • 03:45–08:45
      • 13:45–15:00
      • weekends all day
    If already open, returns 0.
    """
    now = now or datetime.datetime.now()
    wd = now.weekday()  # 0=Mon, …, 6=Sun
    t = now.time()

    # 1) Date of next Monday 08:45 if it is weekend
    if wd >= 5:
        # how many days until next Monday?
        days_ahead = 7 - wd
        next_open_date = (now + datetime.timedelta(days=days_ahead)).date()
        next_open_dt = datetime.datetime.combine(next_open_date, datetime.time(8, 45))
        return (next_open_dt - now).total_seconds()

    # 2) Closed windows today
    morning_start = datetime.time(3, 45)
    morning_end   = datetime.time(8, 45)
    noon_start    = datetime.time(13, 45)
    noon_end      = datetime.time(15, 0)

    # If we’re in the 03:45–08:45 window, next open is today at 08:45
    if morning_start <= t < morning_end:
        next_open_dt = datetime.datetime.combine(now.date(), morning_end)
        return (next_open_dt - now).total_seconds()

    # If we’re in the 13:45–15:00 window, next open is today at 15:00
    if noon_start <= t < noon_end:
        next_open_dt = datetime.datetime.combine(now.date(), noon_end)
        return (next_open_dt - now).total_seconds()

    # Otherwise, market is open now (or it’s after 15:00 and before midnight,
    # which you said is open—so we return 0)
    return 0

def is_market_open(now=None):
    """Return True if now is Mon–Fri and not in the two blocked intervals."""
    now = now or datetime.datetime.now()
    # 0=Mon, 1=Tue, …, 6=Sun
    if now.weekday() >= 5:  
        return False

    t = now.time()
    # blocked slots: 03:45–08:45 and 13:45–15:00
    if datetime.time(3, 45) <= t < datetime.time(8, 45):
        return False
    if datetime.time(13, 45) <= t < datetime.time(15, 0):
        return False

    return True


def market_subscribe(api, contract):
    print("subscribe market..")
    api.quote.subscribe(
        contract,
        quote_type = sj.constant.QuoteType.Tick,
        version = sj.constant.QuoteVersion.v1,
    )

    api.quote.set_on_tick_fop_v1_callback(quote_callback)

def market_unsubscribe(api, contract):
    print("unsubscribe market..")
    api.quote.unsubscribe(
        contract,
        quote_type = sj.constant.QuoteType.Tick,
        version = sj.constant.QuoteVersion.v1,
    )

def order_subscribe(api):
    print("subscribe order..")
    api.set_order_callback(order_callback)




if __name__ == "__main__":

    market_subscribed = False
    order_subscribed = False


    stdval = float(os.environ["WING_STD"])

    # 測試環境登入
    print(os.environ['API_KEY'], os.environ['SECRET_KEY'], os.environ['CA_CERT_PATH'], os.environ['CA_PASSWORD'])
    api = sj.Shioaji(simulation=False)
    accounts = api.login(
        api_key=os.environ["API_KEY"],
        secret_key=os.environ["SECRET_KEY"]
    )
    # 顯示所有可用的帳戶
    print(f"Available accounts: {accounts}")
    api.activate_ca(
        ca_path=os.environ["CA_CERT_PATH"],
        ca_passwd=os.environ["CA_PASSWORD"],
    )

    coh = ComboOrderHandler()
    mkt = market()

    mxf_contract = api.Contracts.Futures.MXF.MXFR1
    opt_contracts=api.Contracts.Options.TX2
    options = get_options(opt_contracts)
    total_calls = sorted(list(options['C'].keys()))
    total_puts = sorted(list(options['P'].keys()))
    
    contract_open = float(os.environ['CONTRACT_OPEN'])
    
    

    while mkt.close is None:
        
        print("mxf data not coming in ...")
        if is_market_open() == False:
            sleep_time = seconds_until_next_open()
            print(f"Sleep {sleep_time} until market open")
            time.sleep(sleep_time)

            market_subscribed = False
            order_subscribed = False
        else:
            market_subscribe(api, mxf_contract)
            order_subscribe(api)
            time.sleep(1)

            market_subscribed = True
            order_subscribed = True

    while contract_open is None:
        contract_open = mkt.close

    std_prices = calculate_ranges(contract_open, stdval)
    co_p3 = combo_order(opt_contracts, std_prices[7], 0, total_calls, options)
    co_p2 = combo_order(opt_contracts, std_prices[6], 0, total_calls, options)
    co_p1 = combo_order(opt_contracts, std_prices[5], 0, total_calls, options)
    co_pp5 = combo_order(opt_contracts, std_prices[4], 0, total_calls, options)
    co_np5 = combo_order(opt_contracts, std_prices[3], 1, total_puts, options)
    co_n1 = combo_order(opt_contracts, std_prices[2], 1, total_puts, options)
    co_n2 = combo_order(opt_contracts, std_prices[1], 1, total_puts, options)
    co_n3 = combo_order(opt_contracts, std_prices[0], 1, total_puts, options)
    

    combo_orders = {
        'co_p3' : {
            'trigger_price' : std_prices[7],
            'side' : '>=',
            'enter_order' : co_p3,
            'stop_order' : co_p2,
            'open_q' : 16,
            'close_q' : 8,
            'triggered' : False,
        },
        'co_p2' : {
            'trigger_price' : std_prices[6],
            'side' : '>=',
            'enter_order' : co_p2,
            'stop_order' : co_p1,
            'open_q' : 8,
            'close_q' : 4,
            'triggered' : False,
        },
        'co_p1' : {
            'trigger_price' : std_prices[5],
            'side' : '>=',
            'enter_order' : co_p1,
            'stop_order' : co_pp5,
            'open_q' : 4,
            'close_q' : 2,
            'triggered' : True,
        },
        'co_pp5' : {
            'trigger_price' : std_prices[4],
            'side' : '>=',
            'enter_order' : co_pp5,
            'stop_order' : None,
            'open_q' : 2,
            'close_q' : 0,
            'triggered' : True,
        },
        'co_np5' : {
            'trigger_price' : std_prices[3],
            'side' : '<=',
            'enter_order' : co_np5,
            'stop_order' : None,
            'open_q' : 1,
            'close_q' : 0,
            'triggered' : False,
        },
        'co_n1' : {
            'trigger_price' : std_prices[2],
            'side' : '<=',
            'enter_order' : co_n1,
            'stop_order' : co_np5,
            'open_q' : 2,
            'close_q' : 1,
            'triggered' : False,
        },
        'co_n2' : {
            'trigger_price' : std_prices[1],
            'side' : '<=',
            'enter_order' : co_n2,
            'stop_order' : co_n1,
            'open_q' : 4,
            'close_q' : 2,
            'triggered' : False,
        },
        'co_n3' : {
            'trigger_price' : std_prices[0],
            'side' : '<=',
            'enter_order' : co_n3,
            'stop_order' : co_n2,
            'open_q' : 8,
            'close_q' : 4,
            'triggered' : False,
        },
    }

    combo_orders = calculate_order_prices(combo_orders)

    for order_name, order_info in combo_orders.items():
        print(f"Order : {order_name}, trigger point : {order_info['trigger_price']}\n")
        print(f"For Entry : Leg1 -> {order_info['enter_order'].c1.name}, Leg2 -> {order_info['enter_order'].c2.name}\n")
        print(f"Entry start price / quantity : {order_info['open_price']} / {order_info['open_q']}\n")
        if order_info['stop_order'] is not None:
            print(f"For Close : Leg1 -> {order_info['stop_order'].c1.name}, Leg2 -> {order_info['stop_order'].c2.name}\n")
            print(f"CLose start price /quantity : {order_info['close_price']} / {order_info['close_q']}\n")
        print('-'*20)


    all_triggered = False
    order_trial_limit = 200
    order_waited_time = 0.5
    main_loop_sleep = 1  # Sleep 1 second between main loop iterations
    prev_close = None
    print(send_to_telegram(BOT_TOKEN, CHAT_ID, "wing strategy start"))
    while all_triggered == False:
        time.sleep(main_loop_sleep)  # Add sleep to reduce CPU usage

        if is_market_open() == False:

            market_unsubscribe(api, mxf_contract)
            order_subscribe(api)
            market_subscribed = False
            order_subscribed = False

            sleep_time = seconds_until_next_open()
            print(f"Sleep {sleep_time} until market open")
            time.sleep(sleep_time)
            continue
        else:
            if market_subscribed == False:
                market_subscribe(api, mxf_contract)
                market_subscribed = True
            if order_subscribed == False:
                order_subscribe(api)
                order_subscribed = True
            
            time.sleep(1)
        
            
        cur_close = mkt.close
        # Skip processing if price hasn't changed
        if cur_close == prev_close:
            continue
            
        prev_close = cur_close
        placing_trial_time = 1
        covering_trail_time = 1
        
        msg_cnt = 0
        
        something_triggered = False

        for contract_name, order in combo_orders.items():
            if ops[order['side']](cur_close, order['trigger_price']) and order['triggered'] == False:
                send_to_telegram(BOT_TOKEN, CHAT_ID, f"{contract_name} is triggered")
                something_triggered = True

                open_price = order['open_price']
                close_price = order['close_price']
                open_q = order['open_q']
                close_q = order['close_q']
                print(f"{contract_name} is triggered with {order['trigger_price']} and {open_price} / {open_q}")
                while placing_trial_time < order_trial_limit:
                    if msg_cnt%10 == 0:
                        placing_order(api, order['enter_order'], open_price, open_q, 'open')
                    time.sleep(order_waited_time)
                    coh.evaluate()
                    if coh.status == 'filled':
                        combo_orders[contract_name]['triggered'] = True
                        send_to_telegram(BOT_TOKEN, CHAT_ID, f"{contract_name} orders are all filled!")
                        #send open filled message to telegram
                        break
                    elif coh.status == 'partial':
                        open_q = combo_orders[contract_name]['open_q'] = coh.left_q
                        send_to_telegram(BOT_TOKEN, CHAT_ID, f"{contract_name} orders are partial filled, {open_q} lefted!")
                        #send partial filled message to telegram
                    
                    if placing_trial_time % 50 == 0:
                        open_price -= 1
                    
                    placing_trial_time+=1
                    msg_cnt+=1


                msg_cnt = 0
                while covering_trail_time < order_trial_limit and order['stop_order'] is not None:
                    if msg_cnt%10 == 0:
                        send_to_telegram(BOT_TOKEN, CHAT_ID, f"closing {contract_name} close order")
                    placing_order(api, order['stop_order'], close_price, close_q, 'close')
                    time.sleep(order_waited_time)
                    coh.evaluate()
                    if coh.status == 'filled':
                        send_to_telegram(BOT_TOKEN, CHAT_ID, f"{contract_name} previous order are all filled!")
                        #send close filled message to telegram
                        break
                    elif coh.status == 'partial':
                        close_q = combo_orders[contract_name]['close_q'] = coh.left_q
                        send_to_telegram(BOT_TOKEN, CHAT_ID, f"{contract_name} previous order are partial filled, {close_q} lefted!")
                        #send partial filled message to telegram
                    if covering_trail_time %50 == 0:
                        close_price+=1

                    covering_trail_time+=1
                    msg_cnt+=1
        
        if something_triggered and (placing_trial_time == order_trial_limit or covering_trail_time == order_trial_limit):
            if placing_trial_time == order_trial_limit:
                send_to_telegram(BOT_TOKEN, CHAT_ID, f"{contract_name} order cant be all filled!")
            elif covering_trail_time == order_trial_limit:
                send_to_telegram(BOT_TOKEN, CHAT_ID, f"{contract_name} prev order cant be all filled!")
            #send message to telegram for notifying something wrong
            pass

        all_triggered = reduce(lambda i, j : i & j, [order['triggered'] for name, order in combo_orders.items()])

    send_to_telegram(BOT_TOKEN, CHAT_ID, f"All wing strategy orders got triggered!")
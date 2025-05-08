import os
import math
import pickle
import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm


def preprocess(df):
    df = df[['交易日期', '契約', '到期月份(週別)', '開盤價', '最高價', '最低價', '收盤價','成交量', '結算價', '未沖銷契約數', '是否因訊息面暫停交易', '交易時段']]
    df = df.rename(columns={
        '交易日期' :'date',
        '契約' : 'contract', 
        '到期月份(週別)' : 'expire', 
        '開盤價' : 'open', 
        '最高價' : 'high', 
        '最低價' : 'low', 
        '收盤價' : 'close', 
        '成交量' : 'volume', 
        '結算價' : 'final_close', 
        '未沖銷契約數' : 'oi', 
        '是否因訊息面暫停交易' : 'terminate', 
        '交易時段' : 'trade_time'
    })
    df = df[df['contract'] == 'MTX']
    df = df[~df['expire'].str.contains('/', na=False)]
    df = df[df['expire'].str.contains('W', na=False)]
    df['date'] = df['date'].apply(lambda x : datetime.datetime.strptime(x, "%Y/%m/%d"))
    df['weekday'] = df['date'].apply(lambda x : x.strftime("%A"))
    float_cols = [
        'open', 'high', 'low', 'close', 'volume', 'final_close', 'oi',
    ]

    for col in float_cols:
        df[col] = df[col].replace("-", np.nan)
        df[col] = df[col].astype(float)

    return df


def collect_weekly_amp(df):
    df = df[df['weekday']=='Wednesday']
    diffs = []
    df['after_market_open'] = np.nan
    df.loc[df['trade_time'] == '盤後', 'after_market_open'] = df.loc[df['trade_time'] == '盤後', 'open']
    for unique_expire in tqdm(df['expire'].unique()):
        tmp = df[df['expire'] == unique_expire]
        tmp['after_market_open'] = tmp['after_market_open'].ffill()
        val = tmp.loc[tmp['final_close']==0, 'close'] - tmp.loc[tmp['final_close']==0, 'after_market_open'] 
        if len(val) > 0:
            diff = val.values[0]
            if not np.isnan(diff):
                diffs.append(diff)
    return diffs

if __name__ == "__main__":
    data = []
    path = "./data"
    files = os.listdir(path)
    files = [f for f in files if f != '.DS_Store']
    for file in files:
        fullpath = os.path.join(path, file)
        df = pd.read_csv(fullpath)
        df = preprocess(df)
        data.append(df)

    fulldf = pd.concat(data)
    diffs = collect_weekly_amp(fulldf)

    std_val = np.std(diffs)

    info = {
        "recorded_files" : files,
        "std_val" : std_val
    }

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"./info_{timestamp}.pkl", "wb") as file:
        pickle.dump(info, file)
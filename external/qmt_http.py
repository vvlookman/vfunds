import numpy as np
import pandas as pd
import sys
import uvicorn
import xtquant
from datetime import datetime
from fastapi import FastAPI, HTTPException
from xtquant import xtdata

app = FastAPI()


def df_to_json(df):
    df = df.replace([np.nan, np.inf, -np.inf], None)
    return df.to_dict(orient="records")


@app.get("/detail/{stock}")
def detail(stock: str):
    data = xtdata.get_instrument_detail(stock_code=stock)

    return data


@app.get("/dividend/{stock}")
def dividend(stock: str):
    df = xtdata.get_divid_factors(stock_code=stock)
    df['date'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize(
        'UTC').dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')

    return df_to_json(df)


@app.get("/kline/{stock}")
def kline(stock: str, period: str = '1d', dividend_type: str = 'front_ratio'):
    xtdata.download_history_data2(stock, period)
    data = xtdata.get_market_data(
        stock_list=[stock], dividend_type=dividend_type)

    df = pd.concat([df.T for df in data.values()], axis=1)
    df.columns = data.keys()
    df['date'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize(
        'UTC').dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')

    return df_to_json(df)


@app.get("/report/{stock}")
def report(stock: str, table: str = 'PershareIndex'):
    xtdata.download_financial_data2(stock_list=[stock], table_list=[table])
    data = xtdata.get_financial_data(stock_list=[stock], table_list=[table])

    df = data[stock][table]
    df['date'] = df['m_anntime'].str.replace(
        r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3', regex=True)

    return df_to_json(df)


@app.get("/")
def root():
    return {"python": sys.version, "xtquant": getattr(xtquant, "__version__", "unknown")}


if __name__ == '__main__':
    uvicorn.run('qmt_http:app', host='0.0.0.0', port=9000, reload=True)

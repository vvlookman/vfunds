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

    if 'time' in df.columns:
        df['date'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize(
            'UTC').dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')

    return df_to_json(df)


@app.get("/index_weight/{index}")
def index_weight(index: str):
    xtdata.download_index_weight()
    data = xtdata.get_index_weight(index_code=index)

    return data


@app.get("/kline/{stock}")
def kline(stock: str, period: str = '1d', dividend_type: str = 'none'):
    xtdata.download_history_data2(stock, period)
    data = xtdata.get_market_data_ex(
        stock_list=[stock], dividend_type=dividend_type)

    if stock in data:
      df = data[stock]

       if 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize(
                'UTC').dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')

        return df_to_json(df)
    else:
        return []


@app.get("/report/{stock}")
def report(stock: str, table: str = 'PershareIndex'):
    xtdata.download_financial_data2(stock_list=[stock], table_list=[table])
    data = xtdata.get_financial_data(stock_list=[stock], table_list=[table])

    if stock in data and table in data[stock]:
        df = data[stock][table]

        if 'm_anntime' in df.columns:
            df['date'] = df['m_anntime'].str.replace(
                r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3', regex=True)

        return df_to_json(df)
    else:
        return []


@app.get("/")
def root():
    return {"python": sys.version, "xtquant": getattr(xtquant, "__version__", "unknown")}


if __name__ == '__main__':
    uvicorn.run('qmt_http:app', host='0.0.0.0', port=9000, reload=True)

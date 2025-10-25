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


@app.get("/index_weight/{index}")
def index_weight(index: str):
    xtdata.download_index_weight()
    data = xtdata.get_index_weight(index_code=index)

    return data


@app.get("/stock_detail/{stock}")
def stock_detail(stock: str):
    data = xtdata.get_instrument_detail(stock_code=stock)

    return data


@app.get("/stock_dividend/{stock}")
def stock_dividend(stock: str):
    df = xtdata.get_divid_factors(stock_code=stock)

    if 'time' in df.columns:
        df['date'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize(
            'UTC').dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')

    return df_to_json(df)


@app.get("/stock_kline/{stock}")
def stock_kline(stock: str, period: str = '1d', dividend_type: str = 'none'):
    xtdata.download_history_data2(stock, period)
    data = xtdata.get_market_data_ex(
        stock_list=[stock], dividend_type=dividend_type)

    if stock in data:
        df = data[stock]

        if 'time' in df.columns:
            df['date'] = pd.to_datetime(df['time'], unit='ms').dt.tz_localize(
                'UTC').dt.tz_convert('Asia/Shanghai').dt.strftime('%Y-%m-%d')

        if df.empty:
            raise HTTPException(status_code=500)
        else:
            return df_to_json(df)
    else:
        raise HTTPException(status_code=404)


@app.get("/stock_report/{stock}")
def stock_report(stock: str, table: str = 'PershareIndex'):
    # 'Balance'          # 资产负债表
    # 'Income'           # 利润表
    # 'CashFlow'         # 现金流量表
    # 'Capital'          # 股本表
    # 'Holdernum'        # 股东数
    # 'Top10holder'      # 十大股东
    # 'Top10flowholder'  # 十大流通股东
    # 'PershareIndex'    # 每股指标
    xtdata.download_financial_data2(stock_list=[stock], table_list=[table])
    data = xtdata.get_financial_data(stock_list=[stock], table_list=[table])

    if stock in data and table in data[stock]:
        df = data[stock][table]

        if 'm_anntime' in df.columns:
            df['date'] = df['m_anntime'].str.replace(
                r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3', regex=True)

        return df_to_json(df)
    else:
        raise HTTPException(status_code=404)


@app.get("/stocks_sector")
def stocks_sector(sector_prefix: str = 'SW1'):
    sector_list = [s for s in xtdata.get_sector_list() if s.startswith(
        sector_prefix) and not s.endswith('加权')]

    data = {}
    for sector in sector_list:
        for stock in xtdata.get_stock_list_in_sector(sector):
            data[stock] = sector[len(sector_prefix):]

    return data


@app.get("/")
def root():
    return {"python": sys.version, "xtquant": getattr(xtquant, "__version__", "unknown")}


if __name__ == '__main__':
    uvicorn.run('qmt_http:app', host='0.0.0.0', port=9000, reload=True)

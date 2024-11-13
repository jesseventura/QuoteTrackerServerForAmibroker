from flask import Flask,request,logging

import pandas as pd
import numpy as np
from glob import glob
import os,sys
from joblib import Parallel,delayed
from dat_util import datdir, get_tdays_dirs, get_contract


##############   get last Quote
quote_symbols = list()

def read_ctptick_lastrow(symbol):
    lastday = get_tdays_dirs()[-1]
    possible_fps = [
        os.path.join(f'{datdir}/{lastday}/{symbol}_{lastday}.csv'),
        os.path.join(f'{datdir}/{lastday}/{symbol}_{lastday}.csv.bz2'),
        os.path.join(f'{datdir}/{lastday}/{symbol.lower()}_{lastday}.csv'),
        os.path.join(f'{datdir}/{lastday}/{symbol.lower()}_{lastday}.csv.bz2'),
    ]
    for fp in possible_fps:
        try:
            df = pd.read_csv(fp)
            break
        except:
            continue
    open = df.LastPrice.iloc[0]
    df = df.tail(2)
    # logger.debug(df)
    df['local_timestamp'] = pd.to_datetime(df.local_timestamp).dt.strftime('%m/%d/%Y %H:%M:%S')
    df['Date'] = df.local_timestamp.apply(lambda s:s.split(' ')[0].strip())
    df['Time'] = df.local_timestamp.apply(lambda s:s.split(' ')[1].strip())
    df['Change'] = df.LastPrice.diff()
    df['Tick'] = df.Change.apply(lambda x: 'U' if x>0 else 'D')
    df['LastVolume'] = df.CumVolume.diff()
    df['Open'] = open


    ret = [str(df.InstrumentID.iloc[-1]),  str(df.Date.iloc[-1]),  str(df.Time.iloc[-1]),
           round(df.LastPrice.iloc[-1],6), round(df.BidPrice.iloc[-1],6),   round(df.AskPrice.iloc[-1],6),
           round(df.Change.iloc[-1],6),   str(df.Tick.iloc[-1]),
           round(df.CumVolume.iloc[-1],6),
           0,0,
           round(df.BidVolume.iloc[-1],6), round(df.AskVolume.iloc[-1],6),  round(df.LastVolume.iloc[-1],6),
           0,0,round(open,6),0,0,

           ]
    return ','.join([str(item) for item in ret])

def get_lastquote():
    retstr_lines = ['OK']
    # quote_symbols = get_all_symbols().split('\r\n')[1].strip().split(',')
    retlines = Parallel(n_jobs=20,verbose=5)(delayed(read_ctptick_lastrow)(fp) for fp in quote_symbols)
    retstr_lines += retlines
    return '\r\n'.join(retstr_lines)

##############   get time Sales
def read_ctptick_csv(fp):
    df = pd.read_csv(fp)
    return df[['local_timestamp','InstrumentID','BidPrice','AskPrice','LastPrice','CumVolume','OpenInterest']]


def get_timesales(symbol,fromtimestr,totimestr):
    retstr_lines = ['OK']
    lastday = get_tdays_dirs()[-1]
    possible_fps = [
        os.path.join(f'{datdir}/{lastday}/{symbol}_{lastday}.csv'),
        os.path.join(f'{datdir}/{lastday}/{symbol}_{lastday}.csv.bz2'),
        os.path.join(f'{datdir}/{lastday}/{symbol.lower()}_{lastday}.csv'),
        os.path.join(f'{datdir}/{lastday}/{symbol.lower()}_{lastday}.csv.bz2'),
    ]
    for fp in possible_fps:
        try:
            df = read_ctptick_csv(fp)
            break
        except:
            continue
    df['local_timestamp'] = pd.to_datetime(df.local_timestamp)
    df['local_timestamp_str'] = df.local_timestamp.dt.strftime('%m/%d/%Y %H:%M:%S')
    df['Date'] = df.local_timestamp_str.apply(lambda s: s.split(' ')[0].strip())
    df['Time'] = df.local_timestamp_str.apply(lambda s: s.split(' ')[1].strip())

    df['LastVolume'] = df.CumVolume.diff()
    df.loc[df.index[0], 'LastVolume'] = df.loc[df.index[0], 'CumVolume']
    df = df[df.LastVolume.ne(0)]

    for k, v in df.iterrows():
        _t = int(v['local_timestamp'].strftime('%H%M%S'))
        # if _t < int(fromtimestr):
        #     continue
        # elif _t > int(totimestr) and int(totimestr) != 0:
        #     continue
        ret = [str(v['Date']), str(v['Time']),
               round(v['LastPrice'],6), round(v['AskPrice'],6), round(v['BidPrice'],6),
               # crypto/forex could have fractal volume
               round(v['LastVolume'],6),
               ]
        retstr_lines.append(','.join([str(item) for item in ret]))
    return '\r\n'.join(retstr_lines)


    
    
app = Flask(__name__)

# from ADK.zip(amibroker development kit)'s QT Plugin Source File
# implementing following requests

# oURL.Format("http://%s:%d/req?EnumSymbols(ACTIVE)");
# oURL.Format("http://%s:%d/req?getLastQuote(ACTIVE)");
# oURL.Format("http://%s:%d/req?GetTimeSales(%s,%d,0)", g_oServer, g_nPortNumber, pszTicker, nStartTime );
# oURL.Format("http://%s:%d/req?AddStocksToPort(CURRENT,%s)", g_oServer, g_nPortNumber, pszTicker );


def get_all_symbols(datdir=datdir,product_type='FUT',ret_df=False):
    '''

    :param datdir:
    :param type: 'FUT','OPT',
    :return:
    '''
    retstr_lines = ['OK']
    lastday = get_tdays_dirs(datdir)[-1]
    today_contract_df = pd.DataFrame([
        get_contract(item) for item in sorted(glob(os.path.join(datdir, lastday,'*.csv*')))])
    # get_contract('MA003P2025_20191223'),get_contract('i2002-P-620_20191223')

    sel_df = today_contract_df[today_contract_df['contract_class'] == product_type]
    # fps = sorted(glob(f'{datdir}/{lastday}/*.csv'))
    # syms = sorted([item.split('_')[0] for item in fps])

    retstr_lines += [','.join(sel_df.original_contract_code)]
    if ret_df:
        return sel_df
    else:
        return '\r\n'.join(retstr_lines)


@app.route("/")
def read_1():
    return 'OK'

logger = logging.create_logger(app)

@app.route("/req")
def read_2():
    func_params =  request.query_string.decode()

    logger.info(func_params)
    retstr = func_params
    func,respara = func_params.split('(')
    paras = respara.split(')')[0].split(',')
    if func == 'EnumSymbols':
        if len(paras) == 1:
            if paras[0] == 'ACTIVE':
                retstr = get_all_symbols()
        elif len(paras) == 0:
            pass

    elif func == 'getLastQuote':
        if len(paras) == 1:
            if paras[0] == '*':
                pass
            elif paras[0] == 'ACTIVE':
                retstr = get_lastquote()
            elif paras[0] == 'CURRENT':
                pass
    elif func == 'GetTimeSales':

        if len(paras) == 3:
            symbol,fromtimestr,totimestr = paras
            retstr = get_timesales(symbol,fromtimestr,totimestr)

    elif func == 'AddStocksToPort':
        if len(paras) == 2:
            portname, symbol = paras
            if symbol not in quote_symbols:
                quote_symbols.append(symbol)
    return retstr


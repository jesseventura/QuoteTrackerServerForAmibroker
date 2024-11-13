import os, sys

datdir = os.path.expanduser('~/data_recorder/')


# zmq sub -> file sink: realtime dat file example:  "~/data_recorder/20241113/IF2412_20241113.csv{.bz2}"
# dat format is from a zmqStream of a topicAddress
# ActionDay,AskPrice,AskPrice1,AskPrice2,AskPrice3,AskPrice4,AskPrice5,AskVolume,AskVolume1,AskVolume2,AskVolume3,AskVolume4,AskVolume5,AveragePrice,BidPrice,BidPrice1,BidPrice2,BidPrice3,BidPrice4,BidPrice5,BidVolume,BidVolume1,BidVolume2,BidVolume3,BidVolume4,BidVolume5,CumVolume,ExchangeID,ExchangeInstID,InstrumentID,LastPrice,LowerLimitPrice,OpenInterest,PreClosePrice,PreOpenInterest,PreSettlementPrice,SettlementPrice,TradingDay,Turnover,UpdateTimeLong,UpperLimitPrice,local_timestamp,msgType,topicAddr
# 20241113,4060.2,4060.2,0.0,0.0,0.0,0.0,2,2,0,0,0,0,1218060.0,4060.0,4060.0,0.0,0.0,0.0,0.0,2,2,0,0,0,0,67,,,IF2411,4060.2,3674.2,39052.0,4081.8,39091.0,4082.4,0.0,20241113,81610020.0,09:29:00.200,4490.6,2024-11-13 09:29:00.330266,3,MarketDataReal@CTPFuture/IF2411
# 20241113,4063.4,4063.4,0.0,0.0,0.0,0.0,3,3,0,0,0,0,1218125.06024096,4062.0,4062.0,0.0,0.0,0.0,0.0,15,15,0,0,0,0,83,,,IF2411,4060.8,3674.2,39052.0,4081.8,39091.0,4082.4,0.0,20241113,101104380.0,09:30:00.200,4490.6,2024-11-13 09:30:00.392637,3,MarketDataReal@CTPFuture/IF2411


################ helper functions
def get_tdays_dirs(datdir=datdir):
    tdays = list()
    for fn in sorted(os.listdir(datdir)):
        #         sdir =
        # new dat format starts from 20191219
        if fn < '20180101':
            continue
        if os.path.isdir(os.path.join(datdir, fn)) and len(fn) == 8:
            tdays.append(fn)
    return tdays


def get_productID(code, ):
    for idx, s in enumerate(code):
        if s.isdigit():
            return code[:idx]
    return ''


def replace_underlying_con(opt_under_prefix):
    rep_map = {
        'IO': 'IF',
        'MO': 'IM',
        'HO': 'IH',
    }
    if opt_under_prefix in rep_map.keys():
        return rep_map[opt_under_prefix]
    return opt_under_prefix


def get_opt_underlying_strike(opt_code, callputchar):
    suffix = opt_code[2:].upper()
    x = suffix.find(callputchar)
    if x < 0:
        raise Exception('no c or p found')
    strike_str = suffix[x + 1:]
    under = replace_underlying_con(opt_code[:2]) + suffix[:x]
    return under, int(strike_str)


def get_contract(fp):
    fn = os.path.basename(fp)
    contract_code_ori, tday = fn.split('.csv')[0].split('_')
    contract_code = contract_code_ori.replace('-', '')
    # first 2 chars productid?
    suffix = contract_code[2:].upper()
    info = dict()
    info['fp'] = fp
    info['tday'] = tday
    info['original_contract_code'] = contract_code_ori
    if 'C' in suffix:
        info['iscall'] = True
        info['contract_class'] = 'OPT'
    elif 'P' in suffix:
        info['iscall'] = False
        info['contract_class'] = 'OPT'
    else:
        info['contract_class'] = 'FUT'

    info['productID'] = get_productID(contract_code)
    if info['contract_class'] == 'OPT':
        if info['iscall']:
            underlying, strike = get_opt_underlying_strike(contract_code, 'C')
        else:
            underlying, strike = get_opt_underlying_strike(contract_code, 'P')
        info['underlying'] = underlying
        info['strike'] = strike
    elif info['contract_class'] == 'FUT':
        info['underlying'] = contract_code
    return info

import pandas as pd
from pykrx import stock
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import warnings
warnings.filterwarnings( 'ignore' )
from io import BytesIO


def get_investor_netbuy(st, stddate):
    M3_ago = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=3), "%Y%m%d"))
    if st == '1001' : mkt = ['STK']
    elif st == '2001' : mkt = ['KSQ']
    df_all = pd.DataFrame()
    for i in mkt :
        idx_dict = {'개인' : '8000',
                    '기관 합계': '7050',
                    '외국인': '9000'}
        df = pd.DataFrame()
        for keys, values in enumerate(idx_dict.items()):
            query_str_parms = {
                'locale': 'ko_KR',
                'mktId': i,
                'invstTpCd': values[1],
                'strtDd': M3_ago,
                'endDd': stddate,
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false',
                'name': 'fileDown',
                'url': 'dbms/MDC/STAT/standard/MDCSTAT02401'
                    }
            headers = {
                'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0'
                }
            r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
            form_data = {
                'code': r.content
                }
            r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
            df_temp = pd.read_excel(BytesIO(r.content))
            df_temp['구분'] = i
            df_temp['투자자구분'] = values[0]
            df_temp = df_temp[['구분','투자자구분','종목코드','종목명','거래대금_순매수']]
            df = pd.concat([df, df_temp]).reset_index(drop=True)
        df_all = pd.concat([df_all, df]).reset_index(drop=True)
    return df_all

def get_idx_universe(stddate) :
    idx_dict = {
        '코스피200제외' : '244',
        '코스닥150': '203'}
    df_all = pd.DataFrame()
    for keys, values in enumerate(idx_dict.items()):
        if values[0][:3] == '코스피': st = '1'
        else : st = '2'
        query_str_parms = {
        'locale': 'ko_KR',
        'tboxindIdx_finder_equidx0_0': values[0],
        'indIdx': st,
        'indIdx2': values[1],
        'codeNmindIdx_finder_equidx0_0': values[0],
        'param1indIdx_finder_equidx0_0': '',
        'trdDd': stddate,
        'money': '3',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00601'
            }
        headers = {
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0'
            }
        r = requests.get('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', query_str_parms, headers=headers)
        form_data = {
            'code': r.content
            }
        r = requests.post('http://data.krx.co.kr/comm/fileDn/download_excel/download.cmd', form_data, headers=headers)
        df = pd.read_excel(BytesIO(r.content))
        for i in range(0, len(df.종목코드)):
            df.종목코드.iloc[i] = str(df.종목코드[i]).zfill(6)
        df['날짜'] = datetime.strftime(datetime.strptime(stddate, "%Y%m%d"), "%Y-%m-%d")
        df['구분'] = values[0]
        df['상장시가총액'] = df['상장시가총액'] * 1000000
        df = df[['날짜', '구분', '종목코드', '종목명', '종가', '등락률', '상장시가총액']]
        df_all = pd.concat([df_all, df]).reset_index(drop=True)
    return df_all

def get_universe(end_date) :
    st = ['1001','2001']
    df_mkt = pd.DataFrame()
    for s in range(0,len(st)) :
        df_temp = get_investor_netbuy(st[s], end_date)
        mktcap = get_idx_universe(end_date)[['종목코드', '상장시가총액', '구분']]
        df = pd.merge(df_temp[['투자자구분', '종목코드', '종목명', '거래대금_순매수']], mktcap, on='종목코드', how='inner')
        df['지분변동'] = df['거래대금_순매수'] / df['상장시가총액']
        df_frn = df[df.투자자구분 == '외국인'].sort_values('지분변동', ascending=False).reset_index(drop=True)[0:20]
        df_ind = df[df.투자자구분 == '개인'].sort_values('지분변동', ascending=False).reset_index(drop=True)[0:20]
        df_ins = df[df.투자자구분 == '기관 합계'].sort_values('지분변동', ascending=False).reset_index(drop=True)[0:20]
        df_all = pd.concat([df_frn, df_ind, df_ins])
        if st[s] == '1001':
            df_all['구분'] = '코스피'
        elif st[s] == '2001':
            df_all['구분'] = '코스닥'
        df_mkt = pd.concat([df_mkt, df_all])
    return df_mkt


def get_bdate_info(start_date, end_date) :
    end_bdate = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(end_date, "%Y%m%d") + relativedelta(days=3),"%Y%m%d"))
    date = pd.DataFrame(stock.get_previous_business_days(fromdate=start_date, todate=end_bdate)).rename(columns={0: '일자'})
    prevbdate = date.shift(1).rename(columns={'일자': '전영업일자'})
    date = pd.concat([date, prevbdate], axis=1).fillna(
        datetime.strftime(datetime.strptime(stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(start_date, "%Y%m%d") - relativedelta(days=1), "%Y%m%d")), "%Y%m%d"),"%Y-%m-%d %H:%M:%S"))
    date['주말'] = ''
    for i in range(0, len(date) - 1):
        if abs(datetime.strptime(datetime.strftime(date.iloc[i + 1].일자, "%Y%m%d"), "%Y%m%d") - datetime.strptime(datetime.strftime(date.iloc[i].일자, "%Y%m%d"), "%Y%m%d")).days > 1:
            date['주말'].iloc[i] = 1
        else:
            date['주말'].iloc[i] = 0
    month_list = date.일자.map(lambda x: datetime.strftime(x, '%Y-%m')).unique()
    monthly = pd.DataFrame()
    for m in month_list:
        try:
            monthly = monthly.append(date[date.일자.map(lambda x: datetime.strftime(x, '%Y-%m')) == m].iloc[-1])
        except Exception as e:
            print("Error : ", str(e))
        pass
    date['월말'] = np.where(date['일자'].isin(monthly.일자.tolist()), 1, 0)
    date = date[date.일자 <= datetime.strftime(datetime.strptime(end_date, "%Y%m%d"),"%Y-%m-%d")]
    return date

start_date = '20170101'
end_date = '20220720'

def get_pf_netbuy(start_date, end_date):
    bdate =  get_bdate_info(start_date, end_date)
    bdate_m = bdate[bdate.월말 == 1].reset_index(drop=True)
    univ = pd.DataFrame()
    for i in range(0,len(bdate_m)) :
        univ_temp = get_universe(datetime.strftime(bdate_m.일자[i], "%Y%m%d"))
        univ_temp['일자'] = datetime.strftime(bdate_m.일자[i], "%Y-%m-%d")
        univ = pd.concat([univ,univ_temp])
    return univ
univ = get_pf_netbuy(start_date, end_date)

univ.to_excel('C:/Users/ysj/Desktop/univv.xlsx')
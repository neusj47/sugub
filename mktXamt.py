import warnings
warnings.filterwarnings( 'ignore' )
import math
from krx_cr import *

# mkt X amt 전략
# 0. 타겟 시장 결정
# 1. 주체별 수급 계산
# 2. 시장, 주체별 순매수 상위 선별

# df = stock.get_index_ohlcv_by_date(start_date, end_date, "1001") 코스피
# df = stock.get_index_ohlcv_by_date(start_date, end_date, "2001") 코스닥
# df = stock.get_index_ohlcv_by_date(start_date, end_date, "1028") 코스피200
# df = stock.get_index_ohlcv_by_date(start_date, end_date, "2203") 코스닥150

def get_mkt_signal(tgt1,tgt2,end_date) :
    M3_ago = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(end_date, "%Y%m%d") - relativedelta(months=3), "%Y%m%d"))
    df_kospi = stock.get_index_ohlcv_by_date(M3_ago, end_date, tgt1).reset_index(drop=False)[['날짜','종가']].rename(columns = {'날짜':'StdDate', '종가':tgt1})
    df_kosdaq = stock.get_index_ohlcv_by_date(M3_ago, end_date, tgt2).reset_index(drop=False)[['날짜','종가']].rename(columns = {'날짜':'StdDate', '종가':tgt2})
    df_target = pd.merge(df_kospi, df_kosdaq, on ='StdDate', how = 'inner')
    df_target['Ln1'] =  df_target[tgt1].apply(lambda x: math.log(float(x)))
    df_target['Ln2'] =  df_target[tgt2].apply(lambda x: math.log(float(x)))
    df_target['Sprd'] = df_target['Ln1'] - df_target['Ln2']
    df_target['Mean'] = df_target['Sprd'].mean()
    df_target['Stdev'] = df_target['Sprd'].std()
    df_target['Diff'] = df_target['Sprd'] - df_target['Mean']
    df_target['Signal'] = np.where(df_target['Diff'] > df_target['Stdev']*0.5,tgt1,np.where(df_target['Diff']<-df_target['Stdev']*0.5,tgt2,''))
    df_target = df_target.sort_values('StdDate').reset_index(drop=True)
    signal = df_target.iloc[len(df_target)-1]['Signal']
    return df_target, signal
# signal = get_mkt_signal(tgt1,tgt2,end_date)[0]
# mkt_target = get_mkt_signal(tgt1,tgt2,end_date)[1]

def get_subject_signal(mkt_target, end_date):
    M3_ago = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(end_date, "%Y%m%d") - relativedelta(months=3), "%Y%m%d"))
    if mkt_target[:1] == '2' : st = 'KSQ'
    elif mkt_target[:1] == '1' : st = 'STK'
    elif mkt_target == '' : st = 'STK'
    else : st = 'STK'
    query_str_parms = {
        'locale': 'ko_KR',
        'inqTpCd': 2,
        'trdVolVal': 2,
        'askBid': 3,
        'mktId': st,
        'strtDd': M3_ago,
        'endDd': end_date,
        'money': '3',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT02202'
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
    df = pd.read_excel(BytesIO(r.content)).sort_values('일자')
    dfs = pd.DataFrame({'StdDate': datetime.strftime(datetime.strptime(end_date, "%Y%m%d") , "%Y-%m-%d"),
                        '기관 합계': df['기관 합계'].sum(),
                        '개인 합계': df['개인'].sum(),
                        '외국인 합계': df['외국인 합계'].sum()}, index = range(0,1))
    if (dfs['기관 합계'][0] > dfs['개인 합계'][0]) & (dfs['기관 합계'][0] > dfs['외국인 합계'][0]) : sub_target = '기관'
    elif (dfs['외국인 합계'][0] > dfs['기관 합계'][0]) & (dfs['외국인 합계'][0] > dfs['개인 합계'][0]) : sub_target = '외국인'
    elif (dfs['개인 합계'][0] > dfs['기관 합계'][0]) & (dfs['개인 합계'][0] > dfs['외국인 합계'][0]) : sub_target = '개인'
    else : sub_target = ''
    return sub_target


def get_investor_netbuy(mkt_target, stddate):
    M3_ago = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=3), "%Y%m%d"))
    if mkt_target[:1] == '1' : mkt = ['STK']
    elif mkt_target[:1] == '2' : mkt = ['KSQ']
    else : mkt = ['STK']
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
        '코스피': '001',
        # '코스피 200' : '028',
        '코스닥': '001',
        # '코스닥 150': '203'
    }
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

def get_universe(mkt_target, end_date) :
    df_temp = get_investor_netbuy(mkt_target, end_date)
    mktcap = get_idx_universe(end_date)[['종목코드', '상장시가총액', '구분']]
    df = pd.merge(df_temp[['투자자구분', '종목코드', '종목명', '거래대금_순매수']], mktcap, on='종목코드', how='inner')
    df['지분변동'] = df['거래대금_순매수'] / df['상장시가총액']
    df_frn = df[df.투자자구분 == '외국인'].sort_values('지분변동', ascending=False).reset_index(drop=True)[0:20]
    df_ins = df[df.투자자구분 == '기관 합계'].sort_values('지분변동', ascending=False).reset_index(drop=True)[0:20]
    df_ind = df[df.투자자구분 == '개인'].sort_values('지분변동', ascending=False).reset_index(drop=True)[0:20]
    df_all = pd.concat([df_frn, df_ins, df_ind])
    return df_all

# tgt1 = '1028'
# tgt2 = '2203'
tgt1 = '1001'
tgt2 = '2001'
start_date = '20170101'
end_date = '20220531'

def get_pf(tgt1, tgt2, start_date, end_date):
    bdate =  get_bdate_info(start_date, end_date)
    bdate_m = bdate[bdate.월말 == 1].reset_index(drop=True)
    univ = pd.DataFrame()
    for i in range(0,len(bdate_m)) :
        mkt_target = get_mkt_signal(tgt1,tgt2,datetime.strftime(bdate_m.일자[i], "%Y%m%d"))[1]
        if mkt_target != '' :
            sub_target = get_subject_signal(mkt_target, datetime.strftime(bdate_m.일자[i], "%Y%m%d"))
            if sub_target != '' :
                univ_temp = get_universe(mkt_target, datetime.strftime(bdate_m.일자[i], "%Y%m%d"))
                univ_temp = univ_temp.loc[univ_temp.투자자구분==sub_target]
                univ_temp['일자'] = datetime.strftime(bdate_m.일자[i], "%Y-%m-%d")
                univ = pd.concat([univ, univ_temp])
            else :
                univ_temp = pd.DataFrame()
                univ = pd.concat([univ, univ_temp])
        else :
            univ_temp = pd.DataFrame()
            univ = pd.concat([univ, univ_temp])
    return univ

pf = get_pf(tgt1, tgt2, start_date, end_date)


pf.to_excel('C:/Users/ysj/Desktop/azaㄴ.xlsx')
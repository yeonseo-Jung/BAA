import os
import quantstats as qs
from datetime import timedelta
import dateutil.parser
import pandas as pd
from operator import itemgetter
from dateutil.relativedelta import relativedelta

def load_asset_monthly(asset_name):
    f_path = f'processed/{asset_name}_monthly.pkl'
    if os.path.exists(f_path):
        df = pd.read_pickle(f_path)
        df.period = pd.to_datetime(df.period.astype(str))
        df = df.sort_values(['period'])
    else:
        raise FileExistsError(f"`{asset_name}` 데이터가 존재하지 않습니다.")

    return df

def load_asset(asset_name):
    f_path = f'processed/{asset_name}_processed.pkl'
    if os.path.exists(f_path):
        df = pd.read_pickle(f_path)
        df.period = pd.to_datetime(df.period.astype(str))
        df = df.sort_values(['period'])
    else:
        raise FileExistsError(f"`{asset_name}` 데이터가 존재하지 않습니다.")

    return df

def run_baa_g12(params):
    
    # get paarams data
    try:
        start_date = (dateutil.parser.parse(params['startDate']) + timedelta(hours=9)).strftime("%Y-%m-%d")
    except:
        start_date = params['startDate']
    try:
        end_date = (dateutil.parser.parse(params['endDate']) + timedelta(hours=9)).strftime("%Y-%m-%d")
    except:
        end_date = params['endDate']
        
    agg1_asset_name = params['baaAssets']['agg1']
    agg2_asset_name = params['baaAssets']['agg2']
    agg3_asset_name = params['baaAssets']['agg3']
    agg4_asset_name = params['baaAssets']['agg4']
    agg5_asset_name = params['baaAssets']['agg5']
    agg6_asset_name = params['baaAssets']['agg6']
    agg7_asset_name = params['baaAssets']['agg7']
    agg8_asset_name = params['baaAssets']['agg8']
    agg9_asset_name = params['baaAssets']['agg9']
    agg10_asset_name = params['baaAssets']['agg10']
    agg11_asset_name = params['baaAssets']['agg11']
    agg12_asset_name = params['baaAssets']['agg12']

    dep1_asset_name = params['baaAssets']['dep1']
    dep2_asset_name = params['baaAssets']['dep2']
    dep3_asset_name = params['baaAssets']['dep3']
    dep4_asset_name = params['baaAssets']['dep4']
    dep5_asset_name = params['baaAssets']['dep5']
    dep6_asset_name = params['baaAssets']['dep6']
    dep7_asset_name = params['baaAssets']['dep7']

    can1_asset_name = params['baaAssets']['can1']
    can2_asset_name = params['baaAssets']['can2']
    can3_asset_name = params['baaAssets']['can3']
    can4_asset_name = params['baaAssets']['can4']

    if "abs" in params.keys():
        abs_asset = params["abs"]
    else:
        abs_asset = None
    
    if "bench" in params.keys():
        bench_asset = params["bench"]
    else:
        bench_asset = "SPY"
    
    if "startegy_name" in params.keys():
        strategy_name = params["startegy_name"]
    else:
        strategy_name = "BAA-G12"
    download_filename = f'{strategy_name}.html'

    # load data
    agg1 = load_asset(agg1_asset_name)
    agg1_monthly = load_asset_monthly(agg1_asset_name)
    agg2 = load_asset(agg2_asset_name)
    agg2_monthly = load_asset_monthly(agg2_asset_name)
    agg3 = load_asset(agg3_asset_name)
    agg3_monthly = load_asset_monthly(agg3_asset_name)
    agg4 = load_asset(agg4_asset_name)
    agg4_monthly = load_asset_monthly(agg4_asset_name)
    agg5 = load_asset(agg5_asset_name)
    agg5_monthly = load_asset_monthly(agg5_asset_name)
    agg6 = load_asset(agg6_asset_name)
    agg6_monthly = load_asset_monthly(agg6_asset_name)
    agg7 = load_asset(agg7_asset_name)
    agg7_monthly = load_asset_monthly(agg7_asset_name)
    agg8 = load_asset(agg8_asset_name)
    agg8_monthly = load_asset_monthly(agg8_asset_name)
    agg9 = load_asset(agg9_asset_name)
    agg9_monthly = load_asset_monthly(agg9_asset_name)
    agg10 = load_asset(agg10_asset_name)
    agg10_monthly = load_asset_monthly(agg10_asset_name)
    agg11 = load_asset(agg11_asset_name)
    agg11_monthly = load_asset_monthly(agg11_asset_name)
    agg12 = load_asset(agg12_asset_name)
    agg12_monthly = load_asset_monthly(agg12_asset_name)

    dep1 = load_asset(dep1_asset_name)
    dep1_monthly = load_asset_monthly(dep1_asset_name)
    dep2 = load_asset(dep2_asset_name)
    dep2_monthly = load_asset_monthly(dep2_asset_name)
    dep3 = load_asset(dep3_asset_name)
    dep3_monthly = load_asset_monthly(dep3_asset_name)
    dep4 = load_asset(dep4_asset_name)
    dep4_monthly = load_asset_monthly(dep4_asset_name)
    dep5 = load_asset(dep5_asset_name)
    dep5_monthly = load_asset_monthly(dep6_asset_name)
    dep6 = load_asset(dep6_asset_name)
    dep6_monthly = load_asset_monthly(dep6_asset_name)
    dep7 = load_asset(dep7_asset_name)
    dep7_monthly = load_asset_monthly(dep7_asset_name)


    can1 = load_asset(can1_asset_name)
    can1_monthly = load_asset_monthly(can1_asset_name)
    can2 = load_asset(can2_asset_name)
    can2_monthly = load_asset_monthly(can2_asset_name)
    can3 = load_asset(can3_asset_name)
    can3_monthly = load_asset_monthly(can3_asset_name)
    can4 = load_asset(can4_asset_name)
    can4_monthly = load_asset_monthly(can4_asset_name)


    ticker_book = {
        "attk_1": agg1,
        "attk_2": agg2,
        "attk_3": agg3,
        "attk_4": agg4,
        "attk_5": agg5,
        "attk_6": agg6,
        "attk_7": agg7,
        "attk_8": agg8,
        "attk_9": agg9,
        "attk_10": agg10,
        "attk_11": agg11,
        "attk_12": agg12,
        "dep_1": dep1,
        "dep_2": dep2,
        "dep_3": dep3,
        "dep_4": dep4,
        "dep_5": dep5,
        "dep_6": dep6,
        "dep_7": dep7,
        "cana_1": can1,
        "cana_2": can2,
        "cana_3": can3,
        "cana_4": can4,
    }

    attacks = {
        "attk_1": agg1_monthly,
        "attk_2": agg2_monthly,
        "attk_3": agg3_monthly,
        "attk_4": agg4_monthly,
        "attk_5": agg5_monthly,
        "attk_6": agg6_monthly,
        "attk_7": agg7_monthly,
        "attk_8": agg8_monthly,
        "attk_9": agg9_monthly,
        "attk_10": agg10_monthly,
        "attk_11": agg11_monthly,
        "attk_12": agg12_monthly,
    }

    deps = {
        "dep_1": dep1_monthly,
        "dep_2": dep2_monthly,
        "dep_3": dep3_monthly,
        "dep_4": dep4_monthly,
        "dep_5": dep5_monthly,
        "dep_6": dep6_monthly,
        "dep_7": dep7_monthly,
    }

    canaries = {
        "cana_1": can1_monthly,
        "cana_2": can2_monthly,
        "cana_3": can3_monthly,
        "cana_4": can4_monthly,
    }

    attacks_name = {
        "attk_1": agg1_asset_name,
        "attk_2": agg2_asset_name,
        "attk_3": agg3_asset_name,
        "attk_4": agg4_asset_name,
        "attk_5": agg5_asset_name,
        "attk_6": agg6_asset_name,
        "attk_7": agg7_asset_name,
        "attk_8": agg8_asset_name,
        "attk_9": agg9_asset_name,
        "attk_10": agg10_asset_name,
        "attk_11": agg11_asset_name,
        "attk_12": agg12_asset_name,
    }

    deps_name = {
        "dep_1": dep1_asset_name,
        "dep_2": dep2_asset_name,
        "dep_3": dep3_asset_name,
        "dep_4": dep4_asset_name,
        "dep_5": dep5_asset_name,
        "dep_6": dep6_asset_name,
        "dep_7": dep7_asset_name,
    }

    def cal_13621w(assets: dict):
        # calculate 13621W mommentum score
        for _, mon in assets.items():
            mon['momentum_score'] = mon['momentum_1'] * 12 + mon['momentum_3'] * 4 + mon['momentum_6'] * 2 + mon['momentum_12']
            
    def cal_sma12(assets: dict):
        for _, mon in assets.items():
            mon['sma_12'] = mon["Close"].rolling(window=12).mean()
            
    cal_13621w(canaries)
    cal_sma12(attacks)
    cal_sma12(deps)

    # set trading period
    # sma12 계산을 위해 1년을 더해야 함
    start_date_by_assets = \
        max([
            min(agg1_monthly.period) + relativedelta(years=1)
            , min(agg2_monthly.period) + relativedelta(years=1) 
            , min(agg3_monthly.period) + relativedelta(years=1)
            , min(agg4_monthly.period) + relativedelta(years=1)
            , min(agg5_monthly.period) + relativedelta(years=1) 
            , min(agg6_monthly.period) + relativedelta(years=1)
            , min(agg7_monthly.period) + relativedelta(years=1)
            , min(agg8_monthly.period) + relativedelta(years=1)
            , min(agg9_monthly.period) + relativedelta(years=1)
            , min(agg10_monthly.period) + relativedelta(years=1)
            , min(agg11_monthly.period) + relativedelta(years=1)
            , min(agg12_monthly.period) + relativedelta(years=1)
            , min(dep1_monthly.period) + relativedelta(years=1)
            , min(dep2_monthly.period) + relativedelta(years=1)
            , min(dep3_monthly.period) + relativedelta(years=1)
            , min(dep4_monthly.period) + relativedelta(years=1)
            , min(dep5_monthly.period) + relativedelta(years=1)
            , min(dep6_monthly.period) + relativedelta(years=1)
            , min(dep7_monthly.period) + relativedelta(years=1)
            ])

    end_date_by_assets = \
        min([
            max(agg1_monthly.period)
            , max(agg2_monthly.period)
            , max(agg4_monthly.period)
            , max(agg5_monthly.period)
            , max(agg6_monthly.period)
            , max(agg7_monthly.period)
            , max(agg8_monthly.period)
            , max(agg9_monthly.period)
            , max(agg10_monthly.period)
            , max(agg11_monthly.period)
            , max(agg12_monthly.period)
            , max(dep1_monthly.period)
            , max(dep2_monthly.period)
            , max(dep3_monthly.period)
            , max(dep4_monthly.period)
            , max(dep5_monthly.period)
            , max(dep6_monthly.period)
            , max(dep7_monthly.period)
            ])

    if start_date < str(start_date_by_assets):
        start_date = start_date_by_assets

    if end_date > str(end_date_by_assets):
        end_date = end_date_by_assets

    dates = pd.date_range(start=start_date, end=end_date)
    rebalnce_dates = pd.date_range(start=start_date, end=end_date, freq='M')

    print(f'* start_date: {dates.tolist()[0].strftime("%Y-%m-%d")}\n* end_date  : {dates.tolist()[-1].strftime("%Y-%m-%d")}')
    
    deps_counts, attack_counts = 3, 6
    avg_returns, model_portfolio, model_portfolios  = {}, [], {}
    for i, date in enumerate(dates):
        
        if date in rebalnce_dates:
            # 리밸런싱 하는 날
            
            yyyymm = str(date).replace('-', '')[:6]
            canary_moms = [(n, mon[mon['yyyymm']== yyyymm]['momentum_score'].tolist()[0]) for n, mon in canaries.items()]
            min_canary_mom = min(canary_moms, key=itemgetter(1))
            if min_canary_mom[1] < 0:
                # 카나리 자산군 모멘텀 스코어 하나라도 음수 -> 방어자산군 투자
                dep_moms = [(n, mon[mon['yyyymm']== yyyymm]['sma_12'].tolist()[0]) for n, mon in deps.items()]
                sorted_dep_mom = sorted(dep_moms, key=itemgetter(1), reverse=True)
                final_assets = sorted_dep_mom[:deps_counts]
                final_assets_name = [deps_name[nm] for nm, _ in final_assets]
                
                # apply absolute momentum
                if abs_asset is not None:
                    if abs_asset in final_assets_name:
                        abs_idx = final_assets_name.index(abs_asset)
                        for i in range(abs_idx + 1, len(final_assets_name)):
                            final_assets_name[i] = abs_asset
            else:
                # 카나리 자산군 모멘텀 스코어 모두 양수(혹은 0) -> 공격자산군 투자
                attk_moms = [(n, mon[mon['yyyymm']== yyyymm]['sma_12'].tolist()[0]) for n, mon in attacks.items()]
                sorted_attk_mom = sorted(attk_moms, key=itemgetter(1), reverse=True)
                final_assets = sorted_attk_mom[:attack_counts]
                final_assets_name = [attacks_name[nm] for nm, _ in final_assets]
            
            model_portfolio = final_assets
            model_portfolios[date] = final_assets_name
            
        if len(model_portfolio) > 0:
            # 리밸런싱 후 수익률 계산
            total_profit = 0
            for t, _ in model_portfolio:
                ticker = ticker_book[t]
                try:
                    profit = ticker.loc[date, 'profit']
                except KeyError:
                    profit = 0
                # 동일비중으로 투자 -> 수익률 1/n
                total_profit = total_profit + profit * (1 / len(model_portfolio))
            
            avg_returns[date] = total_profit
            
    avg_return_df = pd.DataFrame(avg_returns, index=['Close']).transpose()
    avg_return_df = avg_return_df.loc[~(avg_return_df==0).all(axis=1)]
    
    # qs.reports.html(avg_return_df.squeeze(), 'SPY', match_dates=True, title="VAA")
    # 위 코드 실행 시 아래 에러 발생
    # TypeError: Cannot compare dtypes datetime64[ns, America/New_York] and datetime64[ns]
    # Solution: 밴치마크 데이터를 직접 지정해주기
    bench = qs.utils.download_returns(bench_asset.upper())
    bench.index = bench.index.tz_convert(None)
    qs.reports.html(avg_return_df.squeeze(), benchmark=bench, match_dates=True, title=strategy_name, download_filename=download_filename)
    
    return avg_returns

def run_baa_g4(params):
    
    # get paarams data
    try:
        start_date = (dateutil.parser.parse(params['startDate']) + timedelta(hours=9)).strftime("%Y-%m-%d")
    except:
        start_date = params['startDate']
    try:
        end_date = (dateutil.parser.parse(params['endDate']) + timedelta(hours=9)).strftime("%Y-%m-%d")
    except:
        end_date = params['endDate']
        
    agg1_asset_name = params['baaAssets']['agg1']
    agg2_asset_name = params['baaAssets']['agg2']
    agg3_asset_name = params['baaAssets']['agg3']
    agg4_asset_name = params['baaAssets']['agg4']

    dep1_asset_name = params['baaAssets']['dep1']
    dep2_asset_name = params['baaAssets']['dep2']
    dep3_asset_name = params['baaAssets']['dep3']
    dep4_asset_name = params['baaAssets']['dep4']
    dep5_asset_name = params['baaAssets']['dep5']
    dep6_asset_name = params['baaAssets']['dep6']
    dep7_asset_name = params['baaAssets']['dep7']

    can1_asset_name = params['baaAssets']['can1']
    can2_asset_name = params['baaAssets']['can2']
    can3_asset_name = params['baaAssets']['can3']
    can4_asset_name = params['baaAssets']['can4']

    if "abs" in params.keys():
        abs_asset = params["abs"]
    else:
        abs_asset = None
    
    if "bench" in params.keys():
        bench_asset = params["bench"]
    else:
        bench_asset = "QQQ"
    
    if "startegy_name" in params.keys():
        strategy_name = params["startegy_name"]
    else:
        strategy_name = "BAA-G4"
    download_filename = f'{strategy_name}.html'

    # load data
    agg1 = load_asset(agg1_asset_name)
    agg1_monthly = load_asset_monthly(agg1_asset_name)
    agg2 = load_asset(agg2_asset_name)
    agg2_monthly = load_asset_monthly(agg2_asset_name)
    agg3 = load_asset(agg3_asset_name)
    agg3_monthly = load_asset_monthly(agg3_asset_name)
    agg4 = load_asset(agg4_asset_name)
    agg4_monthly = load_asset_monthly(agg4_asset_name)

    dep1 = load_asset(dep1_asset_name)
    dep1_monthly = load_asset_monthly(dep1_asset_name)
    dep2 = load_asset(dep2_asset_name)
    dep2_monthly = load_asset_monthly(dep2_asset_name)
    dep3 = load_asset(dep3_asset_name)
    dep3_monthly = load_asset_monthly(dep3_asset_name)
    dep4 = load_asset(dep4_asset_name)
    dep4_monthly = load_asset_monthly(dep4_asset_name)
    dep5 = load_asset(dep5_asset_name)
    dep5_monthly = load_asset_monthly(dep6_asset_name)
    dep6 = load_asset(dep6_asset_name)
    dep6_monthly = load_asset_monthly(dep6_asset_name)
    dep7 = load_asset(dep7_asset_name)
    dep7_monthly = load_asset_monthly(dep7_asset_name)

    can1 = load_asset(can1_asset_name)
    can1_monthly = load_asset_monthly(can1_asset_name)
    can2 = load_asset(can2_asset_name)
    can2_monthly = load_asset_monthly(can2_asset_name)
    can3 = load_asset(can3_asset_name)
    can3_monthly = load_asset_monthly(can3_asset_name)
    can4 = load_asset(can4_asset_name)
    can4_monthly = load_asset_monthly(can4_asset_name)


    ticker_book = {
        "attk_1": agg1,
        "attk_2": agg2,
        "attk_3": agg3,
        "attk_4": agg4,
        "dep_1": dep1,
        "dep_2": dep2,
        "dep_3": dep3,
        "dep_4": dep4,
        "dep_5": dep5,
        "dep_6": dep6,
        "dep_7": dep7,
        "cana_1": can1,
        "cana_2": can2,
        "cana_3": can3,
        "cana_4": can4,
    }

    attacks = {
        "attk_1": agg1_monthly,
        "attk_2": agg2_monthly,
        "attk_3": agg3_monthly,
        "attk_4": agg4_monthly,
    }

    deps = {
        "dep_1": dep1_monthly,
        "dep_2": dep2_monthly,
        "dep_3": dep3_monthly,
        "dep_4": dep4_monthly,
        "dep_5": dep5_monthly,
        "dep_6": dep6_monthly,
        "dep_7": dep7_monthly,
    }

    canaries = {
        "cana_1": can1_monthly,
        "cana_2": can2_monthly,
        "cana_3": can3_monthly,
        "cana_4": can4_monthly,
    }

    attacks_name = {
        "attk_1": agg1_asset_name,
        "attk_2": agg2_asset_name,
        "attk_3": agg3_asset_name,
        "attk_4": agg4_asset_name,
    }

    deps_name = {
        "dep_1": dep1_asset_name,
        "dep_2": dep2_asset_name,
        "dep_3": dep3_asset_name,
        "dep_4": dep4_asset_name,
        "dep_5": dep5_asset_name,
        "dep_6": dep6_asset_name,
        "dep_7": dep7_asset_name,
    }

    def cal_13621w(assets: dict):
        # calculate 13621W mommentum score
        for _, mon in assets.items():
            mon['momentum_score'] = mon['momentum_1'] * 12 + mon['momentum_3'] * 4 + mon['momentum_6'] * 2 + mon['momentum_12']
            
    def cal_sma12(assets: dict):
        for _, mon in assets.items():
            mon['sma_12'] = mon["Close"].rolling(window=12).mean()
            
    cal_13621w(canaries)
    cal_sma12(attacks)
    cal_sma12(deps)

    # set trading period
    # sma12 계산을 위해 1년을 더해야 함
    start_date_by_assets = \
        max([
            min(agg1_monthly.period) + relativedelta(years=1)
            , min(agg2_monthly.period) + relativedelta(years=1) 
            , min(agg3_monthly.period) + relativedelta(years=1)
            , min(agg4_monthly.period) + relativedelta(years=1)
            , min(dep1_monthly.period) + relativedelta(years=1)
            , min(dep2_monthly.period) + relativedelta(years=1)
            , min(dep3_monthly.period) + relativedelta(years=1)
            , min(dep4_monthly.period) + relativedelta(years=1)
            , min(dep5_monthly.period) + relativedelta(years=1)
            , min(dep6_monthly.period) + relativedelta(years=1)
            , min(dep7_monthly.period) + relativedelta(years=1)
            ])

    end_date_by_assets = \
        min([
            max(agg1_monthly.period)
            , max(agg2_monthly.period)
            , max(agg4_monthly.period)
            , max(dep1_monthly.period)
            , max(dep2_monthly.period)
            , max(dep3_monthly.period)
            , max(dep4_monthly.period)
            , max(dep5_monthly.period)
            , max(dep6_monthly.period)
            , max(dep7_monthly.period)
            ])

    if start_date < str(start_date_by_assets):
        start_date = start_date_by_assets

    if end_date > str(end_date_by_assets):
        end_date = end_date_by_assets

    dates = pd.date_range(start=start_date, end=end_date)
    rebalnce_dates = pd.date_range(start=start_date, end=end_date, freq='M')

    print(f'* start_date: {dates.tolist()[0].strftime("%Y-%m-%d")}\n* end_date  : {dates.tolist()[-1].strftime("%Y-%m-%d")}')
    
    deps_counts, attack_counts = 3, 1
    avg_returns, model_portfolio, model_portfolios  = {}, [], {}
    for i, date in enumerate(dates):
        
        if date in rebalnce_dates:
            # 리밸런싱 하는 날
            
            yyyymm = str(date).replace('-', '')[:6]
            canary_moms = [(n, mon[mon['yyyymm']== yyyymm]['momentum_score'].tolist()[0]) for n, mon in canaries.items()]
            min_canary_mom = min(canary_moms, key=itemgetter(1))
            if min_canary_mom[1] < 0:
                # 카나리 자산군 모멘텀 스코어 하나라도 음수 -> 방어자산군 투자
                dep_moms = [(n, mon[mon['yyyymm']== yyyymm]['sma_12'].tolist()[0]) for n, mon in deps.items()]
                sorted_dep_mom = sorted(dep_moms, key=itemgetter(1), reverse=True)
                final_assets = sorted_dep_mom[:deps_counts]
                final_assets_name = [deps_name[nm] for nm, _ in final_assets]
                
                # apply absolute momentum
                if abs_asset is not None:
                    if abs_asset in final_assets_name:
                        abs_idx = final_assets_name.index(abs_asset)
                        for i in range(abs_idx + 1, len(final_assets_name)):
                            final_assets_name[i] = abs_asset
            else:
                # 카나리 자산군 모멘텀 스코어 모두 양수(혹은 0) -> 공격자산군 투자
                attk_moms = [(n, mon[mon['yyyymm']== yyyymm]['sma_12'].tolist()[0]) for n, mon in attacks.items()]
                sorted_attk_mom = sorted(attk_moms, key=itemgetter(1), reverse=True)
                final_assets = sorted_attk_mom[:attack_counts]
                final_assets_name = [attacks_name[nm] for nm, _ in final_assets]
            
            model_portfolio = final_assets
            model_portfolios[date] = final_assets_name
            
        if len(model_portfolio) > 0:
            # 리밸런싱 후 수익률 계산
            total_profit = 0
            for t, _ in model_portfolio:
                ticker = ticker_book[t]
                try:
                    profit = ticker.loc[date, 'profit']
                except KeyError:
                    profit = 0
                # 동일비중으로 투자 -> 수익률 1/n
                total_profit = total_profit + profit * (1 / len(model_portfolio))
            
            avg_returns[date] = total_profit
            
    avg_return_df = pd.DataFrame(avg_returns, index=['Close']).transpose()
    avg_return_df = avg_return_df.loc[~(avg_return_df==0).all(axis=1)]
    
    # qs.reports.html(avg_return_df.squeeze(), 'SPY', match_dates=True, title="VAA")
    # 위 코드 실행 시 아래 에러 발생
    # TypeError: Cannot compare dtypes datetime64[ns, America/New_York] and datetime64[ns]
    # Solution: 밴치마크 데이터를 직접 지정해주기
    bench = qs.utils.download_returns(bench_asset.upper())
    bench.index = bench.index.tz_convert(None)
    qs.reports.html(avg_return_df.squeeze(), benchmark=bench, match_dates=True, title=strategy_name, download_filename=download_filename)
    
    return avg_returns
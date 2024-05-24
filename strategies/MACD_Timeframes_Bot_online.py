# MACD_Timeframes_Bot_online

##################################### Datas #########################################
                        ########## Imports ##########
from os import path
import sys
# # Online Path
# sys.path.append("/home/ubuntu/Online_Bots")
# Offline Path
sys.path.append("D:\Projets_Python\Online_Bots")
import pandas_ta as ta
from datetime import datetime, timedelta

from strategies.Wallet_and_positions_manager import get_wallet, get_positions
from strategies.df_manager_bitget import get_and_manage_df_bitget, load_datas_from_file, save_datas_to_file
from strategies.Trades_Manager import get_closure_order, report_results, search_to_open_position, search_to_move_sl
# save and load datas directory Offline
save_load_datas_directory = r'D:\Projets_Python\Online_Bots\strategies\Datas'
# # save and load datas directory Online
# save_load_datas_directory = r'/home/ubuntu/Online_Bots/strategies/Datas'

                        ########## Time ##########

now = datetime.utcnow()
# Start timer
start_time = now
current_time = now.strftime("%d-%m-%Y %H:%M:%S")
print("                                         _______________________________________________________                                       ")
print("                                         --- Start Execution Time :", current_time, "---")

                        ########## Input ##########

production = True

# Setup
## Product

# # Reel account
# marginCoin = 'ETH'
# contrepartie= "USD"
# productType= "DMCBL"

# Demo account
marginCoin = 'SETH'
contrepartie= "SUSD"
productType= "SDMCBL"

# Money Management
mysize = 1
leverage = 2
stop_loss = 40
trailing_stop = 1.8
take_profit = 150

# nb of candle to check for find lowest
lowest_low_nb = 5

symbol = marginCoin+contrepartie+"_"+productType

# Setup : date start / date end 
# date = obj date
from_date = "18-9-2023"+" 0:00:00" # str
# from_date str --> obj_from_date
obj_from_date = datetime.strptime(from_date, "%d-%m-%Y %H:%M:%S") # objet

# objet automatic for now
obj_to_now = now 

# Intervals short term
interval = "5m"
orderType='market'

# final data filename
df_filename = symbol + "_" + interval + ".xlsx"
print(f"--- {symbol} {interval} Leverage x {leverage} ---")

holdSide = ["long"]

interval2 = "1Dutc"
obj_from_date2=(obj_to_now- timedelta(days=900))

                        ########## Load Datas ##########

# Download datas from df manager
df = get_and_manage_df_bitget(symbol, interval, obj_from_date, obj_to_now, df_filename, save_load_datas_directory, sheet_name='Price_History')
print("                                         -----------------------------------------------------                                       ")

# Download datas Weekly from df manager
df2 = get_and_manage_df_bitget(symbol, interval2, obj_from_date2, obj_to_now, df_filename, save_load_datas_directory, sheet_name='Long_Price_History')
print("                                         -----------------------------------------------------                                       ")

# Download datas from file
df_trades_loaded = load_datas_from_file(df_filename, save_load_datas_directory, sheet_name='Trades_Results')

##################################### End Datas #########################################

############################# Define you signal function ##################################

                        ########## Indicators ##########

# Calculate MACD for different timeframes
macd_5m = ta.macd(df['close']).fillna(0).to_numpy()
macd_15m = ta.macd(df.resample('15min').last()['close']).fillna(0).to_numpy()
macd_1h = ta.macd(df.resample('1H').last()['close'], offset=1).fillna(0).to_numpy()
macd_4h = ta.macd(df.resample('4H').last()['close'], offset=1).fillna(0).to_numpy() # minimum 6000 5 Min

# macd_day = ta.macd(df.resample(('D')).last()['close'], offset=1).fillna(0).to_numpy()
macd_day = ta.macd(df2['close']).fillna(0).to_numpy() # minimum 770 D
macd_week = ta.macd(df2.resample('W').last()['close'], offset=None).fillna(0).to_numpy() # minimum 110 W

                        ########## Buy or Sell Signal ##########

# Signal for MACD open or not pour DF

# TRUE = MACD AU DESSUS de SIGNAL
indicators_name=["macd_week", "macd_day", "macd_4h", "macd_1h", "macd_15m", "macd_5m"]
indicators_list=(macd_week, macd_day, macd_4h, macd_1h, macd_15m, macd_5m)

def cross_macd(df, indicators_list, indicators_name, candle_back):
    results = []
    dt = df.copy()
    for i in indicators_list:
        if i[candle_back][0] > i[candle_back][2]:
            results.append(True)
        else:
            results.append(False)
    result_signals = dict(zip(indicators_name, results))
    dt.loc[dt.index[candle_back], result_signals.keys()] = result_signals.values()
    return dt

# check and save last candles for each Timeframes
df = cross_macd(df, indicators_list, indicators_name, candle_back=-1)

############################## Wallets and Positions ####################################

                        ########## Wallets ##########

# get wallet available
wallet_available = get_wallet(productType)

                        ########## Positions ##########

# Check if position open or not
open_position = get_positions(productType, marginCoin)

##################################### Matrix #########################################

                        ########## Backtest ##########

# backtest bench

# # Ligne -2
df["macd_week"][-2] = True
df["macd_day"][-2] = False
df["macd_4h"][-2] = True
df["macd_1h"][-2] = False
df["macd_15m"][-2] = True
df["macd_5m"][-2] = True
# df["Position_ouverte"][-2] = "4h"

# # Ligne -1
df["macd_week"][-1] = True
df["macd_day"][-1] = True
df["macd_4h"][-1] = True
df["macd_1h"][-1] = True
df["macd_15m"][-1] = True
df["macd_5m"][-1] = True
# df["Position_ouverte"][-1] = "day"

# df_trades_loaded["timeframe"][-1] = "15m"

                        ########## MAIN ##########

if open_position:
    search_to_move_sl(df, symbol, marginCoin, lowest_low_nb, production, trailing_stop, holdSide, indicators_name, df_trades_loaded, candle_back_1=-1, candle_back_2=-2)
    
if not open_position:
    if df_trades_loaded is not None and df_trades_loaded["action"].iloc[-1] == "open":
        # 1- search first if positions was closed on previous candle by SL or TP
        closed_order = get_closure_order(symbol, df_trades_loaded, marginCoin, wallet_available)
        if closed_order is not None:
            closed_order
            # 2- Generate trade report
            report_to_save = report_results(df_trades_loaded, closed_order, indicators_name, marginCoin)
            # 3-save
            report_saved = save_datas_to_file(df_filename, report_to_save, save_load_datas_directory, sheet_name="Trades_Results")
    new_df_trades_saved = search_to_open_position(df, df_filename, symbol, leverage, mysize, take_profit, stop_loss, indicators_name, wallet_available, production, holdSide, marginCoin, save_load_datas_directory)

##################################### End #########################################

                        ########## Save ##########

save_datas_to_file(df_filename, df, save_load_datas_directory, sheet_name='Price_History')
save_datas_to_file(df_filename, df2, save_load_datas_directory, sheet_name='Long_Price_History')

                        ########## Stop timer ##########

# Stop timer
end_time = datetime.utcnow()
# Calcul total duration for execution in seconds
execution_time = end_time - start_time
current_time = end_time.strftime("%d/%m/%Y %H:%M:%S")
# Show duration in seconds
print(f"Execution total duration : {execution_time} seconds")
print("                                         --- End Execution Time :", current_time, "UTC ---\n")
print("                         ##################################### END #########################################\n")
##################################### END #########################################

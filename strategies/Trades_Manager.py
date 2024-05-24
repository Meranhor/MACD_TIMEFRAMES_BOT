# Trades_Manager.py

""" Work only in timeframe 5 min
"""
################################################################# Imports ###########################################################

import sys
from datetime import datetime, timedelta, timezone
from settings import TELEGRAM_API_TOKEN, TELEGRAM_CHAT_ID
from telegram import send_telegram_message

from strategies.Connect_to_Bitget_Serveur import Order, Plan, Market, Account
from strategies.df_manager_bitget import df_trades_datas, load_datas_from_file, save_datas_to_file, concat_old_and_new_trades_datas
from strategies.Connect_to_Bitget_Serveur import Order, Plan, Market, Account

################################################################# Trades ################################################################

################################################################# Open Trades ###########################################################

# set leverage
def set_leverage(symbol, marginCoin, leverage, holdSide='long'):
    new_leverage = Account.leverage(symbol, marginCoin, str(leverage), holdSide)
    # format "leverage": "20"
    try:
        if new_leverage['msg'] == "success" and new_leverage['data']:# mettre la réf de la réponse 
            new_leverage_set = int(new_leverage['data']['longLeverage'])
            print("Leverage level set to 1:"+ str(new_leverage_set))
            return new_leverage_set
    except:
        print("ATTENTION !!! Leverage level was not changed due to an error")

# save trade ID
def save_trade_id(new_order):
    try:
        if new_order['data']:         
            order_id = new_order['data']["orderId"]
            return order_id
    except(IndexError):
            print("ERROR: No trade number !!!")

# Find actual market price and retrn it
def actual_long_market_price(symbol):
    found_long_market_price = Market.fills(symbol, limit="1")['data'][0]['price']
    return found_long_market_price

# only send order to take trade to serveur
def send_order_to_open_trade(symbol, marginCoin, trade_setup, holdSide, take_profit, stop_loss):
    new_order = Order.place_order(
    symbol, 
    marginCoin, 
    size=str(trade_setup[1]), 
    side="open_"+str(holdSide[0]), 
    orderType='market', 
    clientOrderId=None, 
    price='', 
    timeInForceValue='normal', 
    presetTakeProfitPrice=str(trade_setup[0]+take_profit), 
    presetStopLossPrice=str(trade_setup[0]-stop_loss)
    )
    return new_order

# Setup trade
def setup_trade(symbol, wallet_available, marginCoin, mysize, new_leverage):
    market_price_now = float(actual_long_market_price(symbol))
    long_quantity_in_eth = round((wallet_available[marginCoin] * mysize)*new_leverage, 2)
    trade_size = max(long_quantity_in_eth, 0.1) # mini 0.1 eth sur Bitget
    return market_price_now, trade_size
    # Resultat market_price_now = trade_setup[0] 
    # long_quantity_in_eth= trade_setup[1]
    # trade_size = trade_setup[2]

# get datas from trade ID
def get_datas_with_order_id(symbol, order_id):
    asked_datas = Order.fills(symbol, order_id)
    # print("order data =",asked_datas)
    order_datas = { 'cTime': asked_datas['data'][0]['cTime'][:-3],
                    'order_id': asked_datas['data'][0]['orderId'],
                    'tradeSide': asked_datas['data'][0]['tradeSide'][5:], 
                    'sizeQty': asked_datas['data'][0]['sizeQty'],
                    'fillAmount': asked_datas['data'][0]['fillAmount'],
                    'price': asked_datas['data'][0]['price'],
                    'fee': asked_datas['data'][0]['fee'],
}
    return order_datas

# format trade datas (open)
def format_trade_datas(datas_from_trade_just_opended, marginCoin, new_leverage, timeframe, wallet_available):
    # ATTENTION 3 h removed from "date" due to convertion UTC
    trade_datas_formated = {
        "date" : datetime.utcfromtimestamp(int(datas_from_trade_just_opended['cTime'])),
        "length": "",
        "order_id" : int(datas_from_trade_just_opended['order_id']),
        "action" : 'open',
        "tradeSide" : datas_from_trade_just_opended['tradeSide'],
        "timeframe" : timeframe,
        "size" : float(datas_from_trade_just_opended['sizeQty']),
        "leverage" : new_leverage,
        "value" : float(datas_from_trade_just_opended['fillAmount']),
        "open_price" : float(datas_from_trade_just_opended['price']),
        "close_price" : float(),
        "price_delta" : float(),
        "delta_in_%" : float(),
        "fees_$" : (abs(float(datas_from_trade_just_opended['fee'])) * float(datas_from_trade_just_opended['price'])),
        marginCoin +"_wallet" : float(wallet_available[marginCoin]),
        marginCoin +"_equity_$" : float(wallet_available[marginCoin + " usdtEquity"]),
        "equity_delta_$" : float(),
        "equity_delta_%" : float(),
    }
    return trade_datas_formated

# process_to_open_trade
def process_to_open_trade(symbol, marginCoin, leverage, timeframe, wallet_available, mysize, holdSide, take_profit, stop_loss):
    # timeframe format = day, 4H, etc
    if timeframe == "day":
        new_leverage = set_leverage(symbol, marginCoin, leverage=5, holdSide='long') # leverage
    else :
        new_leverage = set_leverage(symbol, marginCoin, leverage, holdSide='long') # leverage
    trade_setup = setup_trade(symbol, wallet_available, marginCoin, mysize, new_leverage) # market_price_now, long_quantity_in_eth, trade_size
    new_order_ref = send_order_to_open_trade(symbol, marginCoin, trade_setup, holdSide, take_profit, stop_loss) # new_order
    order_id = save_trade_id(new_order_ref) # order ID
    # fonction track trades 
    datas_from_trade_just_opended = get_datas_with_order_id(symbol, order_id)
    trades_datas_formated = format_trade_datas(datas_from_trade_just_opended, marginCoin, new_leverage, timeframe, wallet_available)
    df_trades_ref = df_trades_datas(trades_datas_formated)
    messages = "Time: " + str(trades_datas_formated["date"]) + " " + trades_datas_formated["tradeSide"] + " market order opened for " + str(trades_datas_formated["size"]) + " " + symbol[:3] + " ~" + str(trades_datas_formated["value"])+ "$" + " at price of " + str(trades_datas_formated["open_price"]) + "$" + " on " + trades_datas_formated["timeframe"] + " Signal"
    print(messages)
    send_telegram_message(TELEGRAM_API_TOKEN, TELEGRAM_CHAT_ID, messages)
    return df_trades_ref

# Crossover for bullish or bearish MACD for trades
## 1 = Bullish crossover, 2 = Bearish crossover, 3 = No crossover
def cross_signal(df, indicator, candle_back_1=-1, candle_back_2=-2):
    cross = 3

    if df.iloc[candle_back_1, df.columns.get_loc(indicator)] and not df.iloc[candle_back_2, df.columns.get_loc(indicator)]:
        print("Crossover", indicator, "Bullish")
        cross = 1
    elif not df.iloc[candle_back_1, df.columns.get_loc(indicator)] and df.iloc[candle_back_2, df.columns.get_loc(indicator)]:
        print("Crossover", indicator, "Bearish")
        cross = 2
    else:
        # print("No crossover actualy")
        # print("")
        cross = 3
    return cross

# V5
# search fonction for crossing MACD signal
def search_signal_for_open_trade(df, indicators_name, candle_back_1=-1, candle_back_2=-2):
    print("No active position ")
    for i in range(len(indicators_name)):
        if cross_signal(df, indicators_name[i], candle_back_1, candle_back_2) == 1:
            if i == 0:
                print(indicators_name[i], "open, waiting to take a trade")
            elif i > 0 and all(df[indicators_name[j - 1]].iloc[candle_back_1] for j in range(1, i + 1)):
                print(indicators_name[i], "open, take position", indicators_name[i])
                return indicators_name[i][5:]
            
# Setup trade parametres
def search_to_open_position(df, df_filename, symbol, leverage, mysize, take_profit, stop_loss, indicators_name, wallet_available, production, holdSide, marginCoin, save_load_datas_directory):
    timeframe = search_signal_for_open_trade(df, indicators_name, candle_back_1=-1, candle_back_2=-2)
    if timeframe and "long" in holdSide:
        if wallet_available[marginCoin] > 0.1: # try if enough money for this trade
            if production:
                # trade opening
                new_df_trades =  process_to_open_trade(symbol, marginCoin, leverage, timeframe, wallet_available, mysize, holdSide, take_profit, stop_loss)
                if new_df_trades is not None:
                    df_trades_loaded = load_datas_from_file(df_filename, save_load_datas_directory, sheet_name='Trades_Results')
                    df_trades_to_save = concat_old_and_new_trades_datas(df_trades_loaded, new_df_trades)
                    new_df_trades_saved = save_datas_to_file(df_filename, df_trades_to_save, save_load_datas_directory, sheet_name="Trades_Results")
                    return new_df_trades_saved
        else:
            print("Not enough funds to take the trade, wallet =", wallet_available+"ETH")
    else:
        print('No signal for new trades')

################################################################# SL position ###########################################################

# move_sl_order
def move_sl_order(df, symbol, marginCoin, lowest_low_nb, lowest_low_multiplier, production, trailing_stop, holdSide, following_position):
    lowest_low = min(df.low[-((lowest_low_nb*lowest_low_multiplier)+1):-2])
    # actual_trade_id = search_actual_trade_id(symbol, isPlan='profit_loss')
    triggerPrice = round((lowest_low - trailing_stop),2)
    # place Other SL for All
    new_order_sl = Plan.place_tpsl(symbol, marginCoin, triggerPrice, planType="pos_loss", holdSide=holdSide[0])
    if new_order_sl['msg'] == "success":
        messages = "New Stop loss positioned at price :" + str(triggerPrice) + "$" + " on signal " + str(following_position)
        print(messages)
        send_telegram_message(TELEGRAM_API_TOKEN, TELEGRAM_CHAT_ID, messages)
    else: 
        print("Stop loss positioning ERROR")

# search timeframe to follow
def search_timeframe_to_follow(df_column_to_analyse, indicators_name):
    position_ouverte = df_column_to_analyse
    if position_ouverte != "" :
        actual_position_index = indicators_name.index("macd_"+position_ouverte) # nb of place in indicators_name
        if actual_position_index != indicators_name[0] or actual_position_index != indicators_name[5]:
            following_position = indicators_name[actual_position_index+1] # calcul following timeframe
            return following_position  # return indicators_name following

        elif actual_position_index == indicators_name[5]:
            following_position = indicators_name[actual_position_index] # calcul actual timeframe
            return following_position # return indicators_name following

# search setup for SL
def search_to_move_sl(df, symbol, marginCoin, lowest_low_nb, production, trailing_stop, holdSide, indicators_name, df_trades_loaded, candle_back_1=-1, candle_back_2=-2):
    # df_trades_loaded = load_datas_from_file(df_filename, sheet_name='Trades_Results')
    following_position = search_timeframe_to_follow(df_trades_loaded["timeframe"].iloc[-1], indicators_name)

    # build dictionary for connect following_position to lowest_low_multiplier
    position_multiplier_mapping = {
        indicators_name[1]: 288,
        indicators_name[2]: 48,
        indicators_name[3]: 12,
        indicators_name[4]: 3,
        indicators_name[5]: 1
    }

    # Check if following_position exist in dictionary
    if following_position in position_multiplier_mapping:
        if cross_signal(df, following_position, candle_back_1, candle_back_2) == 2:
            lowest_low_multiplier = position_multiplier_mapping[following_position]
            move_sl_order(df, symbol, marginCoin, lowest_low_nb, lowest_low_multiplier, production, trailing_stop, holdSide, following_position)

################################################################# END SL position ###########################################################


################################################################# Fonction check past orders ###########################################################

# request previous orders history
def request_previous_orders(symbol, df_trades_loaded, isPlan='profit_loss'):
    # ATTENTION +timedelta(hours=3) due to Hour in UTC
    # version Server
    startTime = str(round(datetime.timestamp(df_trades_loaded.index[-1])*1000))
    endTime = str(round(datetime.timestamp(datetime.now())*1000))
    # # version PC
    # startTime = str(round(datetime.timestamp(df_trades_loaded.index[-1]+timedelta(hours=3))*1000))
    # endTime = str(round(datetime.timestamp(datetime.now()+timedelta(hours=3))*1000))

    previous_orders_executed = Plan.history_plan(symbol, startTime, endTime, pageSize=100, isPre=True, isPlan=isPlan)
    if previous_orders_executed['data'] != 0:
        return previous_orders_executed['data']
    else :
        print("No closure data founded, Please check date and time")
        print("previous order startTime = ", df_trades_loaded.index[-1], "so in Timestamp = ", startTime)
        print("previous order startTime = ", (datetime.utcnow()), "so in Timestamp = ", endTime)
        print("Actual time UTC", datetime.utcnow())
        sys.exit(1)

    
# help for find index number to liste who contain trades closure infos
def find_index_triggered_trade(previous_orders_executed):
    # loop on list and search for targeted statut
    for index, item in enumerate(previous_orders_executed):
        if item.get("status") == "triggered":
            # print("The item with status 'triggered' was found at index", index)
            return index
        else:
            # print("The item with status 'triggered' was not found in the list", index)
            pass

# check if order ritched was SL or TP
def get_info_SL_or_TP(previous_orders_executed_unformated):
    # if SL
    if previous_orders_executed_unformated["planType"] == "pos_loss":
        close_condition = "SL"
        return close_condition
    # if TP
    elif previous_orders_executed_unformated["planType"] == "profit_plan" or previous_orders_executed_unformated["planType"] == "pos_profit":
        close_condition = "TP"
        return close_condition
    
# format datas for SL or TP (open)
def format_executed_previous_order(previous_orders_executed_unformated, close_condition, marginCoin, wallet_available):
    previous_orders_executed_formated = {
        "date" : datetime.utcfromtimestamp(int(previous_orders_executed_unformated['executeTime'][:-3])),
        "length": "",
        "order_id" : int(previous_orders_executed_unformated['executeOrderId']),
        "action" : str(previous_orders_executed_unformated['side'][:-5] +"_"+ close_condition),
        "tradeSide" : previous_orders_executed_unformated['side'][6:],
        "timeframe" : "",
        "size" : float(previous_orders_executed_unformated['size']),
        "leverage" : float(),
        "value" : float(),
        "open_price" : float(),
        "close_price" : float(previous_orders_executed_unformated['triggerPrice']),
        "price_delta" : float(),
        "delta_in_%" : float(),
        "fees_$" : (),
        marginCoin +"_wallet" : float(wallet_available[marginCoin]),
        marginCoin +"_equity_$" : float(wallet_available[marginCoin + " usdtEquity"]),
        "equity_delta_$" : float(),
        "equity_delta_%" : float(),
    }

    return previous_orders_executed_formated

def get_closure_order(symbol, df_trades_loaded, marginCoin, wallet_available):
    previous_orders_executed = request_previous_orders(symbol, df_trades_loaded, isPlan='profit_loss')
    if previous_orders_executed != 0:
        index_triggered_trade = find_index_triggered_trade(previous_orders_executed)
        previous_orders_executed_unformated = previous_orders_executed[index_triggered_trade]
        print(previous_orders_executed_unformated)
        close_condition = get_info_SL_or_TP(previous_orders_executed_unformated)
        previous_orders_executed_formated = format_executed_previous_order(previous_orders_executed_unformated, close_condition, marginCoin, wallet_available)
        df_triggered_order = df_trades_datas(previous_orders_executed_formated)
        return df_triggered_order
    else:
        print("No orders closed recently")
        return None
    
################################################################# END Fonction check past orders ###########################################################


################################################################# Fonction trade report ###########################################################

# calculate % bonus or loss
def calcul_percent(old_value, new_value):
    percent = round(((new_value-old_value)/old_value)*100, 2)
    # print("i.e. a gain of", str(percent)+"%")
    return percent

# calculate Nb Days, Hour, minutes
def calcul_length(df):

    difference = df.index[-1] - df.index[-2]
    jours = difference.days
    secondes = difference.seconds
    heures = secondes // 3600
    minutes = (secondes % 3600) // 60
    resultat = str(jours)+"D", str(heures)+"H", str(minutes)+"min"
    return resultat

def report_calcul(df, indicators_name, marginCoin):
    if df['action'].iloc[-1][:5] == "close":
        df["length"].iloc[-1] = calcul_length(df.copy())
        df["timeframe"].iloc[-1] = search_timeframe_to_follow(df["timeframe"].iloc[-2], indicators_name)[5:]
        df["price_delta"].iloc[-1] = round(df["close_price"].iloc[-1] - df["open_price"].iloc[-2],2)
        df["delta_in_%"].iloc[-1] = calcul_percent(old_value=df["open_price"].iloc[-2], new_value=df["close_price"].iloc[-1])
        df["equity_delta_$"].iloc[-1] = df[marginCoin +"_equity_$"].iloc[-1] - df[marginCoin +"_equity_$"].iloc[-2]
        df["equity_delta_%"].iloc[-1] = calcul_percent(old_value=df[marginCoin +"_equity_$"].iloc[-2], new_value=df[marginCoin +"_equity_$"].iloc[-1])

        messages = (
            "Position " + df['action'].iloc[-1][:5] + "d" + " at : " + str(df.index[-1]) + " by signal " + df["timeframe"].iloc[-1] + "\n" +
            "opened at price: " + str(df["open_price"].iloc[-2])+"$"+" closed at price: " + str(df["close_price"].iloc[-1])+"$" + "\n" +
            "price difference: " + str(df["price_delta"].iloc[-1])+"$"+ " and result: " + str(df["delta_in_%"].iloc[-1])+"%" + "\n" +
            "Previous wallet: " + str(df[marginCoin + "_equity_$"].iloc[-2])+"$"+ "\n" +
            "New wallet: " + str(round(df[marginCoin + "_equity_$"].iloc[-1], 2))+"$" + " so delta: " + str(round(df["equity_delta_%"].iloc[-1],2))+"%" + "\n"
        )
        print(messages)
        send_telegram_message(TELEGRAM_API_TOKEN, TELEGRAM_CHAT_ID, messages)
    return df

# main fonction generate report
def report_results(df_trades_loaded, new_df_trades, indicators_name, marginCoin):
    df_trades_to_analyse = concat_old_and_new_trades_datas(df_trades_loaded, new_df_trades)
    report_results =  report_calcul(df_trades_to_analyse, indicators_name, marginCoin)
    return report_results

################################################################# END Fonction trade report ###########################################################
#######################################################################################################################################################                                                                  
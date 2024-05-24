# df_manager_bitget

""" Work only in timeframe 5 min
"""
################################################################# Imports ###########################################################

import sys
import os
from os import path
import pandas as pd
import numpy as np
import openpyxl
from strategies.Connect_to_Bitget_Serveur import Market
from datetime import datetime, timedelta, timezone
from tqdm import tqdm

################################################################# Datas ###########################################################
### Inputs
# config : date debut / date fin 
# from_date str --> date_start
# date_start = datetime.strptime(from_date, "%d-%m-%Y %H:%M:%S") # objet

# # to_date str --> date_end
# date_end = datetime.strptime(to_date, "%d-%m-%Y %H:%M:%S") # objet

################################################################# Save / Load Managment ###########################################################

# Load Trades_results from file V3
def load_datas_from_file(df_filename, save_load_datas_directory, sheet_name):
  file_path = os.path.join(save_load_datas_directory, df_filename)
  df_datas_loaded = None
  if path.exists(file_path):
    try:
      df_datas_loaded = pd.read_excel(file_path, sheet_name, index_col='date', na_filter=False, parse_dates=['date'])
      return df_datas_loaded
    except ValueError:
        # print("No sheet '"+ sheet_name +"' found")
        pass

# Save datas_to_file
def save_datas_to_file(df_filename, df_datas_to_save, save_load_datas_directory, sheet_name):
  file_path = os.path.join(save_load_datas_directory, df_filename)
  if path.exists(file_path):
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', datetime_format="%Y-%m-%d %H:%M:%S", if_sheet_exists='replace') as writer:
      df_datas_to_save.to_excel(writer, sheet_name,na_rep="NaN", index=True)   
  else:
    with pd.ExcelWriter(file_path, engine='openpyxl', mode='w', datetime_format="%Y-%m-%d %H:%M:%S") as writer:
      df_datas_to_save.to_excel(writer, sheet_name,na_rep="NaN", index=True)
  print("Save to '"+ sheet_name+"' completed")

################################################################# End of Save / Load Managment ###########################################################

################################################################# Datas Managment ###########################################################

# Format datas
def format_new_data(df, iloc=False):
    df = df.astype(float)
    df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'volume2']
    df = df.set_index(df['date'])
    df.index = pd.to_datetime(df.index, unit='ms')
    # df.index = df.index.tz_localize('UTC').tz_convert('Europe/Sofia')
    # df.index = df.index.tz_localize(None)
    del df['date']
    del df['volume2']
    if iloc==True:
        df["iloc"] = range(0, len(df))
    return df

# compare dates
def compare_dates(old_date, recent_date):
    if old_date <= recent_date:
        return True
    else:
        return False

def check_for_error_in_dates(date, limite_date):
    if compare_dates(date, limite_date):
        return True
    else:
        print("ERREUR !!! time-start :", date, ", can't be bigger than time-end :", limite_date)
        print("END OF THE PROGRAMME")
        sys.exit(1)

# convert_interval_in_timedelta
def convert_interval_in_timedelta(interval):
    list_timedelta = {
        "1m": timedelta(minutes=1),
        "3m": timedelta(minutes=3),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1H": timedelta(hours=1),
        "2H": timedelta(hours=2),
        "4H": timedelta(hours=4),
        "6H": timedelta(hours=6),
        "12H": timedelta(hours=12),
        "1D": timedelta(days=1),
        "3D": timedelta(days=3),
        "1W": timedelta(weeks=1),
        "1M": timedelta(days=30),
        "6Hutc": timedelta(hours=6),
        "12Hutc": timedelta(hours=12),
        "1Dutc": timedelta(days=1),
        "3Dutc": timedelta(days=3),
        "1Wutc": timedelta(weeks=1),
        "1Mutc": timedelta(days=30),
    }
    return list_timedelta.get(interval)

# change trades_data to df_trades_data
def df_trades_datas(trades_datas_formated):
   df_trades_results = pd.DataFrame([trades_datas_formated])
   df_trades_results.set_index('date', inplace=True)
   return df_trades_results

# convert pack 
def convert_diff_to_packs(date_start, date_end, interval_converted_in_timedelta):
    if interval_converted_in_timedelta is not None:
        diff = date_end - date_start
        diff_in_units = int(round(diff / interval_converted_in_timedelta))
        return diff_in_units
    else:
        print("ERROR !!! Check again your interval unit conversion")

# concat to df_trades and df_trades_loaded for df_trades_to_save 
def concat_old_and_new_trades_datas(df_trades_loaded=None, new_df_trades=None):
    if df_trades_loaded is None :
        df_trades_to_save = new_df_trades
    else: 
        df_trades_to_save = pd.concat([df_trades_loaded, new_df_trades], axis=0, join='outer', ignore_index=False, keys=None, levels=None, names=None, verify_integrity=False, sort=False, copy=True)
    return df_trades_to_save

# cute data to dowload in slice
def get_dates_intervals(date_start, date_end, max_candles, interval_converted_in_timedelta):
  diff_total_in_units = convert_diff_to_packs(date_start, date_end, interval_converted_in_timedelta)
  dates_intervals = []
  interval_begin_date = date_start

  while diff_total_in_units > 0:
    nb_units_to_add = max_candles - 1
    if diff_total_in_units < max_candles - 1:
      nb_units_to_add = diff_total_in_units
    interval_end_date = interval_begin_date + interval_converted_in_timedelta * nb_units_to_add
    dates_intervals.append([interval_begin_date, interval_end_date])
    diff_total_in_units -= nb_units_to_add + 1
    interval_begin_date = interval_end_date + interval_converted_in_timedelta
  return dates_intervals

################################################################# Filters ###########################################################

# del row not requested
def filter_dates_in_range(df, date_start, date_end):
    # Create a mask
    mask = (df.index >= date_start) & (df.index <= date_end)
    # Apply mask
    filtered_df = df[mask]
    return filtered_df

# Check nb of row
def check_nb_row(df, date_start, date_end, interval_converted_in_timedelta):
  nb_de_lignes_prevue = convert_diff_to_packs(date_start, date_end, interval_converted_in_timedelta)
  print("Number of planned lines =", nb_de_lignes_prevue,"\n"+ "Actual number of rows in the dataframes =", len(df))
  diff_nb_lignes = nb_de_lignes_prevue-len(df)
  if diff_nb_lignes > 15:
    print(diff_nb_lignes, "rows are missing")
    print("Check your dataframe\n")
    return False
  elif diff_nb_lignes > 0:
    print(diff_nb_lignes, "rows are missing\n")
    return False
  elif diff_nb_lignes == 0:
    print(diff_nb_lignes, "rows are missing, dataframe perfect!\n")
  else:
     print(abs(diff_nb_lignes),"rows are too many, duplicates detected")
     return True

#  obtain locations (iloc) of the duplicates
def get_duplicate_iloc(df):
    iloc_dict = {}
    for i, date in enumerate(df.index):
        if date in iloc_dict:
            iloc_dict[date].append(i)
        else:
            iloc_dict[date] = [i]
        return iloc_dict

# filter values and remove double dates values from serveur in df
def filter_double_dates_klines_values(df):
  test_df = df.index.duplicated()
  count_duplicates = np.count_nonzero(test_df)
  print(f"Number of duplicates in the index before correction : {count_duplicates}")

  duplicate_iloc = get_duplicate_iloc(df)
  # Print locations of the duplicates.
  for date, iloc_list in duplicate_iloc.items():
      if len(iloc_list) > 1:
          print(f"Date {date} is duplicated at iloc positions {iloc_list}")
          
  df = df[~df.index.duplicated(keep='first')]
  test_df = df.index.duplicated()
  count_duplicates = np.count_nonzero(test_df)
  print(f"Number of duplicates in the index after correction : {count_duplicates}")
  return df

# Checking the gap between each date
def dates_gap(df, interval_converted_in_timedelta):
  previous_date = None
  for date in df.index:
      if previous_date is not None:
          time_difference = date - previous_date
          if time_difference != interval_converted_in_timedelta:
              print(f"ATTENTION !!! Error: Unexpected gap between dates {previous_date} and {date}")
      previous_date = date

# Delete last candle if needed
def delete_candle_in_progress(df, date_end, interval_converted_in_timedelta):
    date_end = (date_end-(date_end-date_end.min) % interval_converted_in_timedelta) - interval_converted_in_timedelta
    if not compare_dates(df.index[-1], date_end):
        df = df.drop(df.index[-1])
        print("Candle : ",df.index[-1]+interval_converted_in_timedelta, "deleted !")
    return df

def all_filters(df, date_start, date_end, interval_converted_in_timedelta):
  dt = df.copy()
  # df = filter_inconsistent_klines_values()
  dt = filter_dates_in_range(dt, date_start, date_end)
  check_nb_row(df, date_start, date_end, interval_converted_in_timedelta)
  dt = filter_double_dates_klines_values(dt)
  dates_gap(dt, interval_converted_in_timedelta)
  dt = delete_candle_in_progress(dt, date_end, interval_converted_in_timedelta)
  return dt
############################################################### End Filters ###########################################################

################################################################# Matrix #############################################################

# Call API V4
def coin_api_get_exchange_rates_extended(symbol, interval, date_start, date_end, limit='200', option_after_df=False):
  klines = []
  interval_converted_in_timedelta = convert_interval_in_timedelta(interval)
  dates_intervals = get_dates_intervals(date_start, date_end, int(limit), interval_converted_in_timedelta)
  if len(dates_intervals) > 0 :       
    for i in tqdm(dates_intervals, disable = False, desc ="Downloading price datas"):
        # print(i[0], "---", i[1])
        if option_after_df: 
          klines += Market.candles(symbol, interval, int(datetime.timestamp(((i[0])-interval_converted_in_timedelta).replace(tzinfo=timezone.utc)))*1000, int(datetime.timestamp((i[1]).replace(tzinfo=timezone.utc)))*1000, limit='1000')
        else:
          klines += Market.history_candles(symbol, interval, int(datetime.timestamp((i[0]).replace(tzinfo=timezone.utc)))*1000, int(datetime.timestamp((i[1]).replace(tzinfo=timezone.utc)))*1000, limit='200')
  return klines

def download_data_fast(symbol, interval, date_start, date_end):
    return pd.DataFrame(coin_api_get_exchange_rates_extended(symbol, interval, date_start, date_end, limit='1000', option_after_df=True))

def download_data_slow(symbol, interval, date_start, date_end):
    return pd.DataFrame(coin_api_get_exchange_rates_extended(symbol, interval, date_start, date_end, limit='200', option_after_df=False))

def split_data_fast_slow(symbol, interval, date_start, date_end, interval_converted_in_timedelta, limite_date_to_fast_download):

    print("df_part_1 :", date_start, "---", limite_date_to_fast_download)
    df_part_1 = download_data_slow(symbol, interval, date_start, limite_date_to_fast_download)
    print("df_part_2 :", limite_date_to_fast_download- interval_converted_in_timedelta, "---", date_end - interval_converted_in_timedelta)
    df_part_2 = download_data_fast(symbol, interval, limite_date_to_fast_download- interval_converted_in_timedelta, date_end - interval_converted_in_timedelta)
    return pd.concat([df_part_1, df_part_2])

def check_scenario_date(symbol, interval, date_start, date_end, interval_converted_in_timedelta):
    limite_date_to_fast_download = (datetime.utcnow()-(datetime.utcnow()-datetime.utcnow().min) % convert_interval_in_timedelta(interval)) - timedelta(days=28)
    if compare_dates(limite_date_to_fast_download, date_end) and compare_dates(limite_date_to_fast_download, date_start):
        # download all datas Fast
        # print(date_start, "---", date_end)
        return download_data_fast(symbol, interval, date_start, date_end - interval_converted_in_timedelta)

    elif compare_dates(date_start, limite_date_to_fast_download) and compare_dates(date_end, limite_date_to_fast_download):
        # download all datas Slow
        # print(date_start, "---", date_end)
        return download_data_slow(symbol, interval, date_start, date_end)
  
    elif compare_dates(date_start, limite_date_to_fast_download) and compare_dates(limite_date_to_fast_download, date_end):
        # split datas in part Fast and Slow
        # print(date_start, "---", date_end)
        return split_data_fast_slow(symbol, interval, date_start, date_end, interval_converted_in_timedelta, limite_date_to_fast_download)

# Main function
def get_and_manage_df_bitget(symbol, interval, date_start, date_end, df_filename, save_load_datas_directory, sheet_name='Price_History'):
  # check if from_date before obj_to_now and 
  check_for_error_in_dates(date_start, date_end)
  # check if obj_to_now before now
  check_for_error_in_dates(date_end, datetime.utcnow())

  interval_converted_in_timedelta = convert_interval_in_timedelta(interval)
  date_end = date_end-(date_end-date_end.min) % timedelta(minutes=5)
  
  # 1- read df_filename if exist
  df = load_datas_from_file(df_filename, save_load_datas_directory, sheet_name)
    
  if df is not None:
    saved_data_date_start = df.index[0]
    saved_data_date_end = df.index[-1]

    # # Update data before our DF
    nb_unit_start = convert_diff_to_packs(date_start, saved_data_date_start, interval_converted_in_timedelta)
    if nb_unit_start > 0:
      print("Update",nb_unit_start,"candles at the beginning of the database, from", date_start+ interval_converted_in_timedelta, "to", saved_data_date_start)
      df_start = download_data_slow(symbol, interval, date_start, saved_data_date_start)
      if not df_start.empty:
        df_start_formated = format_new_data(df_start, iloc=False)
        df = pd.concat([df_start_formated, df])
    else:
      print("No data to add before the date of :", date_start)

    # Update data after our DF
    nb_unit_end = convert_diff_to_packs(saved_data_date_end, date_end - interval_converted_in_timedelta, interval_converted_in_timedelta)
    if nb_unit_end > 0:
      print("Update",nb_unit_end,"candles at the end of the database, from", saved_data_date_end, "to", date_end - interval_converted_in_timedelta)
      df_end = download_data_fast(symbol, interval, saved_data_date_end , date_end - interval_converted_in_timedelta)
      if not df_end.empty:
        df_end_formated = format_new_data(df_end, iloc=False)
        df = pd.concat([df, df_end_formated])

    else:
      print("No data to add after the date of :", date_end - interval_converted_in_timedelta)

    # if file doesn't exist
  else:
    print("No data files to load, download new datas from server")
    df = check_scenario_date(symbol, interval, date_start, date_end, interval_converted_in_timedelta)
    df = format_new_data(df, iloc=True)
  
  df = all_filters(df, date_start, date_end, interval_converted_in_timedelta)
  df['iloc'] = range(len(df))


  return df
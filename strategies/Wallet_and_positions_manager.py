# Wallet_and_positions_manager.py

################################################################# Imports ###########################################################

from strategies.Connect_to_Bitget_Serveur import Position, Account

################################################################# Wallet ###########################################################

def list_wallet_available(wallet_crypto):

    list_wallet_available = []

    for r in wallet_crypto:
        # Get value to 'available' and 'usdtEquity'
        available_str = float(r['available'])
        equity_str = float(r['usdtEquity'])

        # check if not empty before convert in float
        if available_str and equity_str :
            available = round(float(available_str), 4)
            equity = round(float(equity_str), 4)
            wallet_available = {r['marginCoin']: available, r['marginCoin'] + " usdtEquity": equity}
            # print('Wallet Available', wallet_available)
            list_wallet_available.append(wallet_available)
    return list_wallet_available

# Wallets crypto
def get_wallet(productType):
    downloaded_wallet_crypto = Account.accounts(productType)
    wallet_crypto = downloaded_wallet_crypto['data']

    wallet_available = list_wallet_available(wallet_crypto)[0]
    # print("list_wallet_available :", wallet_available)
    return wallet_available
    # # Wallets Fiat
    # download_wallet_fiat = Account.accounts(productType) 
    # wallet_fiat = download_wallet_fiat['data']

    # for r in wallet_fiat:
    #     if float(r['available']) :
    #         print(r['marginCoin'],':', r['available'],'=', r['usdtEquity'][:-8],'$')
    
    # return wallet_available

################################################################# END Wallet ###########################################################

################################################################# Positions ###########################################################

def get_positions(productType, marginCoin):
# Check if positions opened
    response = Position.all_position_v2(productType, marginCoin)

    if response['data'] :
        open_position = response['data'][0]
        print('1 current position open')

        position = [{
                    "holdSide":  open_position["holdSide"], 
                    "size": float( open_position["leverage"]) * float( open_position["margin"]),
                    "market_price": open_position['marketPrice'],
                    "usd_size": float(open_position["leverage"]) * float( open_position["margin"]) * float( open_position["marketPrice"]),
                    "open_price":  open_position["averageOpenPrice"], 
                    "liquidationPrice":  open_position["liquidationPrice"],
                    "ROE": float(open_position['unrealizedPL'])
                    }]
        print(f"Current position : {open_position['holdSide']} at the price of {open_position['averageOpenPrice']}$. Size in {marginCoin} : {str(position[0]['size'])[:-8]} = {str(position[0]['usd_size'])[:-10]}$. Actual profit {open_position['unrealizedPL']}%")

    else: 
        open_position = 0
        # print('No current position for now')
    return open_position

################################################################# END Positions ###########################################################
# Connect_to_Bitget_Serveur

################################################################# Imports ###########################################################
import settings

from bitget.client import Client
from bitget.mix.market_api import MarketApi
from bitget.mix.account_api import AccountApi
from bitget.mix.order_api import OrderApi
from bitget.mix.position_api import PositionApi
from bitget.mix.plan_api import PlanApi
# from bitget.mix.trace_api import TraceApi

################################################################# Functions ###########################################################

client = Client(
    api_key = settings.BITGET_API_KEY,
    api_secret_key = settings.BITGET_SECRET,
    passphrase = settings.BITGET_PASSPHRASE,
)

Market = MarketApi(
    api_key = settings.BITGET_API_KEY,
    api_secret_key = settings.BITGET_SECRET,
    passphrase = settings.BITGET_PASSPHRASE,
)

Account = AccountApi(
    api_key = settings.BITGET_API_KEY,
    api_secret_key = settings.BITGET_SECRET,
    passphrase = settings.BITGET_PASSPHRASE,
)

Order = OrderApi(
    api_key = settings.BITGET_API_KEY,
    api_secret_key = settings.BITGET_SECRET,
    passphrase = settings.BITGET_PASSPHRASE,
)

Position = PositionApi(
    api_key = settings.BITGET_API_KEY,
    api_secret_key = settings.BITGET_SECRET,
    passphrase = settings.BITGET_PASSPHRASE,
)

Plan = PlanApi(
    api_key = settings.BITGET_API_KEY,
    api_secret_key = settings.BITGET_SECRET,
    passphrase = settings.BITGET_PASSPHRASE,
)

# Plan = TraceApi(
#     api_key = settings.BITGET_API_KEY,
#     api_secret_key = settings.BITGET_SECRET,
#     passphrase = settings.BITGET_PASSPHRASE,
# )

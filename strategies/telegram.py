# telegram.py
# pyTelegramBotAPI

################################################################# Imports ###########################################################
import telebot

################################################################# Functions ###########################################################

def send_telegram_message(TELEGRAM_API_TOKEN, TELEGRAM_CHAT_ID, messages):
    bot = telebot.TeleBot(TELEGRAM_API_TOKEN)

    bot.send_message(TELEGRAM_CHAT_ID, messages)

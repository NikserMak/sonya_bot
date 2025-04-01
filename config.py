import os
from dotenv import load_dotenv

load_dotenv()

# Конфигурационные параметры
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ADMIN_IDS = list(map(int, os.getenv('ADMIN_IDS', '').replace(' ', '').split(','))) if os.getenv('ADMIN_IDS') else []
DB_NAME = 'sleep_bot.db'
POLL_TIME = '08:00'  # Время отправки опроса (по умолчанию 8 утра)
FACT_TIME = '20:00'  # Время отправки факта/совета (по умолчанию 8 вечера)
# Конфигурация телеграм бота для парсинга Cian.ru
import os

# Токен бота из переменных окружения (безопасность)
BOT_TOKEN = os.getenv('BOT_TOKEN', '')

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не найден в переменных окружения. Установите: export BOT_TOKEN='ваш_токен'")

# Настройки парсинга
CIAN_API_URL = "https://api.cian.ru/commercial-search-offers/desktop/v1/offers/get-offers/"

# Настройки безопасного режима
SAFE_MODE_ENABLED = True  # feature: True - включен, False - выключен

# Параметры поиска по умолчанию
DEFAULT_SEARCH_PARAMS = {
    '_type': 'commercialrent',
    'engine_version': {
        'type': 'term',
        'value': 2,
    },
    'office_type': {
        'type': 'terms',
        'value': [1, 3],  # Офисы
    },
    'is_by_commercial_owner': {
        'type': 'term',
        'value': True,
    },
    'region': {
        'type': 'terms',
        'value': [4927],  # Пермь
    },
    'publish_period': {
        'type': 'term',
        'value': 2592000,  # 30 дней
    },
}

# HTTP заголовки для запросов
HEADERS = {
 'accept': '*/*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=1, i',
    'referer': 'https://perm.cian.ru/cat.php?cats%5B0%5D=commercialLandSale&deal_type=sale&electronic_trading=2&engine_version=2&offer_type=offices&region=4927',
    'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
}

# Альтернативные User-Agent строки для ротации
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/110.0',
]

# Настройки прокси (оставьте пустым если не используете)
PROXY_LIST = [
    # Добавьте свои прокси сюда, например:
    # "http://username:password@proxy-server:port",
    # "http://proxy-server:port",
]

# Настройки задержек для имитации человеческого поведения
DELAY_CONFIG = {
    'enabled': True,  # Включить случайные задержки
    'min_delay': 2.0,  # Минимальная задержка в секундах
    'max_delay': 5.0,  # Максимальная задержка в секундах
}

# Настройки безопасности
SECURITY_CONFIG = {
    'rotate_user_agent': True,  # Ротация User-Agent
    'use_proxy_rotation': bool(PROXY_LIST),  # Автоматически определяется по наличию прокси
    'request_timeout': 30,  # Таймаут запроса в секундах
    'max_retries': 3,  # Максимальное количество повторов при ошибке
    'retry_delay': 10,  # Задержка между повторами в секундах
}

# Настройки файлов
DATA_DIR = "data"
SEEN_OFFERS_FILE = f"{DATA_DIR}/seen_offers.json"
EXCEL_OUTPUT_DIR = f"{DATA_DIR}/reports"

# Настройки бота
MAX_MESSAGE_LENGTH = 4096
ADMIN_USER_IDS = [] 
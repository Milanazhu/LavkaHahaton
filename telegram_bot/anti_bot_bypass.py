#!/usr/bin/env python3
"""
Модуль для обхода защиты от ботов при парсинге Cian.ru
Реализует различные техники маскировки под реального пользователя
"""

import requests
import random
import time
import json
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class AntiBotBypass:
    """Продвинутые техники обхода защиты от ботов"""
    
    def __init__(self):
        self.session = requests.Session()
        self.current_headers = {}
        self.cookies_initialized = False
        
    def get_realistic_headers(self) -> Dict[str, str]:
        """Генерирует реалистичные заголовки браузера"""
        
        # Реалистичные User-Agent от реальных браузеров
        user_agents = [
            # Chrome Windows
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            # Chrome Mac
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            # Firefox
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0',
            # Safari
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        ]
        
        user_agent = random.choice(user_agents)
        
        # Определяем платформу из User-Agent
        if 'Windows' in user_agent:
            platform = '"Windows"'
            sec_ch_ua_platform = '"Windows"'
        elif 'Macintosh' in user_agent:
            platform = '"macOS"'
            sec_ch_ua_platform = '"macOS"'
        else:
            platform = '"Linux"'
            sec_ch_ua_platform = '"Linux"'
            
        # Формируем заголовки
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'origin': 'https://perm.cian.ru',
            'pragma': 'no-cache',
            'referer': 'https://perm.cian.ru/rent/commercial/',
            'sec-ch-ua': f'"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': sec_ch_ua_platform,
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': user_agent,
            'x-requested-with': 'XMLHttpRequest',
        }
        
        return headers
    
    def init_browser_session(self) -> bool:
        """Инициализирует сессию как настоящий браузер"""
        try:
            logger.info("Инициализируем браузерную сессию...")
            
            # Шаг 1: Получаем главную страницу для cookies
            headers = self.get_realistic_headers()
            self.session.headers.update(headers)
            
            main_page_response = self.session.get(
                'https://perm.cian.ru/rent/commercial/',
                timeout=10
            )
            
            if main_page_response.status_code == 200:
                logger.info("✅ Главная страница загружена")
                
                # Небольшая задержка как у реального пользователя
                time.sleep(random.uniform(1, 3))
                
                # Шаг 2: Делаем дополнительный запрос для получения cookies
                filters_response = self.session.get(
                    'https://perm.cian.ru/rent/commercial/?deal_type=rent&offer_type=offices',
                    timeout=10
                )
                
                if filters_response.status_code == 200:
                    logger.info("✅ Фильтры загружены, cookies получены")
                    self.cookies_initialized = True
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Ошибка инициализации сессии: {e}")
            return False
    
    def get_alternative_endpoints(self) -> List[str]:
        """Возвращает список альтернативных API endpoints"""
        return [
            'https://api.cian.ru/search-offers/v2/search-offers-desktop/',
            'https://api.cian.ru/v1/get-offers/',
            'https://api.cian.ru/commercial-search-offers/v1/offers/',
            'https://www.cian.ru/ajax/commercial/get-offers/',
        ]
    
    def get_alternative_params(self) -> List[Dict]:
        """Возвращает альтернативные форматы параметров"""
        return [
            # Формат 1: Упрощенный
            {
                "deal_type": "rent",
                "offer_type": "offices",
                "region": 4927,
                "currency": 2
            },
            
            # Формат 2: Стандартный веб
            {
                "jsonQuery": {
                    "deal_type": "rent",
                    "offer_type": ["office", "free_appointment"],
                    "region": [4927],
                    "_type": "terms"
                }
            },
            
            # Формат 3: Мобильный API
            {
                "deal_type": "2",
                "offer_type": "1,3", 
                "region": "4927",
                "engine_version": "2"
            }
        ]
    
    def make_stealth_request(self, url: str, data: Dict) -> Optional[requests.Response]:
        """Выполняет скрытый запрос с обходом защиты"""
        
        # Инициализируем сессию если нужно
        if not self.cookies_initialized:
            if not self.init_browser_session():
                logger.warning("Не удалось инициализировать браузерную сессию")
        
        # Обновляем заголовки для каждого запроса
        new_headers = self.get_realistic_headers()
        self.session.headers.update(new_headers)
        
        # Добавляем случайную задержку
        delay = random.uniform(2, 8)
        logger.info(f"Задержка перед запросом: {delay:.1f} сек")
        time.sleep(delay)
        
        try:
            response = self.session.post(url, json=data, timeout=15)
            logger.info(f"Запрос выполнен: {response.status_code}")
            return response
            
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}")
            return None
    
    def try_multiple_approaches(self) -> Optional[Dict]:
        """Пробует несколько подходов для получения данных"""
        
        endpoints = self.get_alternative_endpoints()
        param_sets = self.get_alternative_params()
        
        for i, endpoint in enumerate(endpoints):
            for j, params in enumerate(param_sets):
                logger.info(f"Попытка {i+1}.{j+1}: {endpoint}")
                
                response = self.make_stealth_request(endpoint, params)
                
                if response and response.status_code == 200:
                    try:
                        data = response.json()
                        if data and 'data' in data:
                            logger.info("✅ Успешно получены данные!")
                            return data
                    except:
                        continue
                        
                elif response and response.status_code == 403:
                    logger.warning("403: Доступ запрещен, пробуем другой подход")
                    continue
                    
                elif response and response.status_code == 500:
                    logger.warning("500: Внутренняя ошибка сервера")
                    continue
                
                # Задержка между попытками
                time.sleep(random.uniform(3, 7))
        
        logger.error("Все попытки исчерпаны")
        return None
    
    def get_working_data(self) -> Optional[Dict]:
        """Главный метод для получения рабочих данных"""
        logger.info("🔄 Начинаем продвинутый обход защиты...")
        
        # Попытка 1: Множественные подходы
        data = self.try_multiple_approaches()
        if data:
            return data
            
        # Попытка 2: Реинициализация сессии
        logger.info("Реинициализируем сессию...")
        self.cookies_initialized = False
        self.session = requests.Session()
        
        data = self.try_multiple_approaches()
        if data:
            return data
            
        logger.error("❌ Не удалось обойти защиту")
        return None

# Создаем глобальный экземпляр
bypass_system = AntiBotBypass() 
#!/usr/bin/env python3
"""
Браузерная автоматизация для обхода защиты от ботов
Использует Selenium для имитации реального пользователя
"""

import time
import random
import json
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.action_chains import ActionChains
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium недоступен. Установите: pip install selenium")

class BrowserBypass:
    """Обход защиты через реальный браузер"""
    
    def __init__(self):
        self.driver = None
        self.initialized = False
    
    def setup_driver(self) -> bool:
        """Настраивает Chrome драйвер с маскировкой"""
        if not SELENIUM_AVAILABLE:
            return False
            
        try:
            chrome_options = Options()
            
            # Базовые настройки для маскировки
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Реалистичный User-Agent
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            chrome_options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            # Дополнительные настройки для обхода детекции
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-plugins-discovery')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--allow-running-insecure-content')
            
            # Запускаем драйвер
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Скрываем признаки автоматизации
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.initialized = True
            logger.info("✅ Браузер инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка инициализации браузера: {e}")
            return False
    
    def human_like_behavior(self):
        """Имитирует поведение человека"""
        # Случайное движение мыши
        try:
            action = ActionChains(self.driver)
            # Случайные координаты
            x = random.randint(100, 800)
            y = random.randint(100, 600)
            action.move_by_offset(x, y).perform()
            
            # Случайная задержка
            time.sleep(random.uniform(0.5, 2.0))
            
            # Иногда делаем клик в случайном месте
            if random.random() < 0.3:
                action.click().perform()
                time.sleep(random.uniform(0.2, 1.0))
                
        except Exception as e:
            logger.debug(f"Ошибка имитации поведения: {e}")
    
    def get_cian_data_with_browser(self) -> Optional[Dict]:
        """Получает данные через браузер"""
        if not self.initialized:
            if not self.setup_driver():
                return None
        
        try:
            logger.info("🌐 Загружаем страницу Cian через браузер...")
            
            # Шаг 1: Открываем главную страницу
            self.driver.get('https://perm.cian.ru/')
            time.sleep(random.uniform(2, 4))
            
            # Имитируем человеческое поведение
            self.human_like_behavior()
            
            # Шаг 2: Переходим к коммерческой недвижимости
            self.driver.get('https://perm.cian.ru/rent/commercial/')
            time.sleep(random.uniform(3, 6))
            
            # Имитируем прокрутку
            self.driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(random.uniform(1, 2))
            
            # Шаг 3: Пытаемся найти и выполнить AJAX запрос
            logger.info("🔍 Ищем AJAX запросы...")
            
            # Ждем загрузки содержимого
            wait = WebDriverWait(self.driver, 10)
            
            # Перехватываем сетевые запросы
            logs = self.driver.get_log('performance')
            api_data = None
            
            for log in logs:
                message = json.loads(log['message'])
                if message['message']['method'] == 'Network.responseReceived':
                    url = message['message']['params']['response']['url']
                    if 'api.cian.ru' in url and 'offers' in url:
                        logger.info(f"📡 Найден API запрос: {url}")
                        # Здесь можно попытаться получить данные
            
            # Альтернативный способ: парсим данные прямо со страницы
            logger.info("📄 Парсим данные со страницы...")
            
            # Ищем объявления на странице
            offers_elements = self.driver.find_elements(By.CSS_SELECTOR, '[data-name="CardComponent"]')
            
            if offers_elements:
                logger.info(f"Найдено {len(offers_elements)} объявлений на странице")
                return self._parse_page_offers(offers_elements)
            else:
                logger.warning("Объявления не найдены на странице")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка браузерного парсинга: {e}")
            return None
        finally:
            self._cleanup()
    
    def _parse_page_offers(self, elements) -> Dict:
        """Парсит объявления прямо со страницы"""
        offers = []
        
        for i, element in enumerate(elements[:10]):  # Берем первые 10
            try:
                # Извлекаем данные
                offer_data = {
                    'id': f'browser_{i+1}_{int(time.time())}',
                    'title': 'Объявление со страницы',
                    'price': 'Цена уточняется',
                    'area': 'Площадь уточняется',
                    'address': 'Пермь',
                    'source': 'browser_parsing'
                }
                
                # Пытаемся найти конкретные данные
                try:
                    price_elem = element.find_element(By.CSS_SELECTOR, '[data-testid="price"]')
                    if price_elem:
                        offer_data['price'] = price_elem.text
                except:
                    pass
                
                try:
                    address_elem = element.find_element(By.CSS_SELECTOR, '[data-name="AddressComponent"]')
                    if address_elem:
                        offer_data['address'] = address_elem.text
                except:
                    pass
                
                offers.append(offer_data)
                
            except Exception as e:
                logger.debug(f"Ошибка парсинга элемента {i}: {e}")
                continue
        
        return {
            'data': {
                'offers': offers
            },
            'source': 'browser',
            'count': len(offers)
        }
    
    def _cleanup(self):
        """Закрывает браузер"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("🚫 Браузер закрыт")
            except:
                pass
            finally:
                self.driver = None
                self.initialized = False

# Функция для быстрого тестирования
def test_browser_bypass():
    """Тестирует браузерный обход"""
    if not SELENIUM_AVAILABLE:
        print("❌ Selenium не установлен")
        return False
    
    bypass = BrowserBypass()
    data = bypass.get_cian_data_with_browser()
    
    if data:
        print(f"✅ Браузерный парсинг успешен: {data.get('count', 0)} объявлений")
        return True
    else:
        print("❌ Браузерный парсинг не удался")
        return False

# Создаем глобальный экземпляр
browser_bypass = BrowserBypass() if SELENIUM_AVAILABLE else None 
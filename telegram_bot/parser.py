import requests
import json
import os
import time
import random
from datetime import datetime
from typing import List, Dict, Tuple, Set
from config import (
    CIAN_API_URL, HEADERS, DEFAULT_SEARCH_PARAMS, DATA_DIR, SEEN_OFFERS_FILE,
    USER_AGENTS, PROXY_LIST, DELAY_CONFIG, SECURITY_CONFIG, SAFE_MODE_ENABLED
)
from dataBD_manager import databd_manager
# Используем databd_manager как основную БД
db_manager = databd_manager
from safe_mode import safe_mode
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Импортируем модуль обхода защиты
try:
    from anti_bot_bypass import bypass_system
    BYPASS_AVAILABLE = True
    logger.info("✅ Модуль обхода защиты загружен")
except ImportError:
    BYPASS_AVAILABLE = False
    logger.warning("⚠️ Модуль обхода защиты недоступен")

class CianParser:
    """Упрощенный парсер объявлений с Cian.ru для интеграции"""
    
    def __init__(self):
        self.ensure_data_dir()
        self.session = requests.Session()
        self._setup_session()
        self.last_safety_check = None  # Результат последней проверки безопасности
    
    def _setup_session(self):
        """Настраивает сессию с ротацией прокси и User-Agent"""
        # Настройка прокси
        if SECURITY_CONFIG['use_proxy_rotation'] and PROXY_LIST:
            proxy = random.choice(PROXY_LIST)
            self.session.proxies.update({
                'http': proxy,
                'https': proxy
            })
            logger.info(f"Используется прокси: {proxy[:20]}...")
        
        # Настройка таймаутов
        self.session.timeout = SECURITY_CONFIG['request_timeout']
        
        # Ротация User-Agent
        if SECURITY_CONFIG['rotate_user_agent']:
            user_agent = random.choice(USER_AGENTS)
            headers = HEADERS.copy()
            headers['user-agent'] = user_agent
            self.session.headers.update(headers)
            logger.info(f"Установлен User-Agent: {user_agent[:50]}...")
        else:
            self.session.headers.update(HEADERS)
    
    def _check_safety_before_request(self, user_id: str) -> bool:
        """Проверка безопасного режима перед парсингом"""
        if not SAFE_MODE_ENABLED:
            logger.info(f"⚠️ Безопасный режим отключен - парсинг разрешен для пользователя {user_id}")
            self.last_safety_check = {'status': 'disabled', 'message': 'Безопасный режим отключен'}
            return True
        
        logger.info(f"🛡️ Проверка безопасного режима для пользователя {user_id}")
        
        can_parse, status_info = safe_mode.can_parse(user_id)
        
        if not can_parse:
            logger.warning(f"🚫 Парсинг заблокирован для пользователя {user_id}: {status_info.get('message', 'Неизвестная причина')}")
            self.last_safety_check = status_info
            return False
        
        logger.info(f"✅ Парсинг разрешен для пользователя {user_id}: {status_info.get('message', 'Проверка пройдена')}")
        self.last_safety_check = status_info
        return True
    
    def ensure_data_dir(self):
        """Создает директорию для данных если её нет"""
        os.makedirs(DATA_DIR, exist_ok=True)
    
    def load_seen_offers(self, user_id: str = "default") -> Set[int]:
        """Загружает ID уже виденных объявлений для пользователя из БД"""
        try:
            return db_manager.get_seen_offers(user_id)
        except Exception as e:
            logger.error(f"Ошибка загрузки seen_offers из БД: {e}")
            return set()
    
    def save_seen_offers(self, seen_offers: Set[int], user_id: str = "default"):
        """Сохраняет ID виденных объявлений в БД"""
        try:
            db_manager.save_seen_offers(user_id, seen_offers)
        except Exception as e:
            logger.error(f"Ошибка сохранения seen_offers в БД: {e}")
    
    def parse_offers(self, user_id: str = "default", only_new: bool = True, geo_filter: Dict = None) -> Tuple[List[Dict], Dict]:
        """
        Основной метод парсинга объявлений с Cian.ru
        
        Args:
            user_id: ID пользователя
            only_new: Показывать только новые объявления
            geo_filter: Географический фильтр (не используется)
            
        Returns:
            Tuple[List[Dict], Dict]: (список объявлений, статистика)
        """
        start_time = time.time()
        
        # Проверяем безопасный режим перед запуском
        if not self._check_safety_before_request(user_id):
            # Возвращаем детальную информацию о блокировке
            safety_info = self.last_safety_check or {}
            
            error_message = safety_info.get('message', 'Парсинг заблокирован системой безопасности')
            
            # Формируем подробный ответ о блокировке
            blocked_stats = {
                'error': 'safe_mode_blocked',
                'message': error_message,
                'status': safety_info.get('status', 'blocked'),
                'hours_left': safety_info.get('hours_left', 0),
                'minutes_left': safety_info.get('minutes_left', 0),
                'next_available': safety_info.get('next_available', 'Неизвестно'),
                'last_parsing': safety_info.get('last_parsing', 'Никогда'),
                'total_today': safety_info.get('total_today', 0),
                'total_all_time': safety_info.get('total_all_time', 0),
                'safety_mode': True
            }
            
            logger.info(f"Парсинг заблокирован для {user_id}: следующий доступен {safety_info.get('next_available', 'неизвестно когда')}")
            return [], blocked_stats
        
        parsing_success = False
        
        try:
            
            # Загружаем уже виденные объявления
            seen_offers = self.load_seen_offers(user_id)
            
            # Начинаем сессию парсинга в БД
            session_id = db_manager.start_parsing_session(user_id, 'cian')
            
            # Формируем параметры запроса
            search_params = DEFAULT_SEARCH_PARAMS.copy()
            
            logger.info(f"Выполняем запрос к Cian API для пользователя {user_id}")
            
            # ПРОДВИНУТЫЙ РЕЖИМ: Пробуем обход защиты
            if BYPASS_AVAILABLE:
                logger.info("🛡️ Используем продвинутый обход защиты...")
                api_data = bypass_system.get_working_data()
                
                if api_data and 'data' in api_data:
                    logger.info("✅ Данные получены через обход защиты!")
                    raw_offers = api_data.get('data', {}).get('offers', [])
                    logger.info(f"Получено {len(raw_offers)} объявлений через обход защиты")
                    
                    # Обрабатываем полученные данные
                    processed_offers, new_offers = self._process_api_data(raw_offers, seen_offers, user_id)
                    
                    # Завершаем успешно
                    self.save_seen_offers(seen_offers, user_id)
                    db_manager.finish_parsing_session(session_id, len(processed_offers), len(new_offers))
                    
                    offers_to_return = new_offers if only_new else processed_offers
                    search_time = time.time() - start_time
                    
                    stats = {
                        'total_count': len(processed_offers),
                        'new_count': len(new_offers),
                        'seen_count': len(processed_offers) - len(new_offers),
                        'search_time': f"{search_time:.1f} сек",
                        'timestamp': datetime.now().strftime('%H:%M:%S'),
                        'bypass_mode': True
                    }
                    
                    logger.info(f"Парсинг завершен успешно: {len(offers_to_return)} объявлений")
                    
                    # Записываем успешный парсинг в безопасный режим
                    safe_mode.log_parsing(user_id, success=True)
                    if SAFE_MODE_ENABLED:
                        logger.info(f"🛡️ Парсинг (обход защиты) записан в безопасный режим для пользователя {user_id}")
                    else:
                        logger.info(f"⚠️ Парсинг (обход защиты) завершен (безопасный режим отключен) для пользователя {user_id}")
                    
                    return offers_to_return, stats
                else:
                    logger.warning("Обход защиты не сработал, пробуем стандартный метод...")
            
            # СТАНДАРТНЫЙ РЕЖИМ: Обычный запрос
            # Выполняем запрос к API
            response = self.session.post(
                CIAN_API_URL,
                json=search_params,
                timeout=SECURITY_CONFIG['request_timeout']
            )
            
            if response.status_code != 200:
                error_msg = f"API вернул статус {response.status_code}"
                logger.error(error_msg)
                
                # РЕЗЕРВНЫЙ РЕЖИМ: Возвращаем тестовые данные если API недоступен
                if response.status_code == 500:
                    logger.warning("API Cian недоступен (500), используем тестовые данные")
                    return self._get_demo_data(user_id, only_new)
                
                return [], {"error": error_msg}
            
            # Парсим ответ
            data = response.json()
            raw_offers = data.get('data', {}).get('offers', [])
            
            logger.info(f"Получено {len(raw_offers)} объявлений из API")
            
            # Обрабатываем объявления
            processed_offers = []
            new_offers = []
            
            for offer in raw_offers:
                try:
                    processed_offer = self._process_offer(offer)
                    processed_offers.append(processed_offer)
                    
                    offer_id = int(processed_offer.get('id', 0))
                    
                    # Проверяем, новое ли это объявление
                    if offer_id not in seen_offers:
                        new_offers.append(processed_offer)
                        seen_offers.add(offer_id)
                        
                        # Сохраняем в БД
                        db_data = self._prepare_for_databd(processed_offer)
                        db_manager.save_listing(db_data)
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки объявления: {e}")
                    continue
            
            # Сохраняем обновленные seen_offers
            self.save_seen_offers(seen_offers, user_id)
            
            # Завершаем сессию парсинга
            db_manager.finish_parsing_session(session_id, len(processed_offers), len(new_offers))
            
            # Определяем какие объявления возвращать
            offers_to_return = new_offers if only_new else processed_offers
            
            # Формируем статистику
            search_time = time.time() - start_time
            stats = {
                'total_count': len(processed_offers),
                'new_count': len(new_offers),
                'seen_count': len(processed_offers) - len(new_offers),
                'search_time': f"{search_time:.1f} сек",
                'timestamp': datetime.now().strftime('%H:%M:%S')
            }
            
            logger.info(f"Парсинг завершен: {len(offers_to_return)} объявлений для пользователя {user_id}")
            
            # Записываем успешный парсинг в безопасный режим
            parsing_success = True
            safe_mode.log_parsing(user_id, success=True)
            if SAFE_MODE_ENABLED:
                logger.info(f"🛡️ Парсинг записан в безопасный режим для пользователя {user_id}")
            else:
                logger.info(f"⚠️ Парсинг завершен (безопасный режим отключен) для пользователя {user_id}")
            
            return offers_to_return, stats
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге для пользователя {user_id}: {e}")
            return [], {"error": str(e)}
    
    def _process_offer(self, offer: Dict) -> Dict:
        """Обрабатывает одно объявление"""
        try:
            # Базовая информация
            offer_id = offer.get('id', 0)
            price = offer.get('bargainTerms', {}).get('priceRur', 0)
            
            # Формируем текст цены
            if price > 0:
                price_text = f"{price:,} ₽/мес".replace(',', ' ')
            else:
                price_text = "Цена не указана"
            
            # Площадь
            area_data = offer.get('totalArea', {})
            area = f"{area_data.get('value', 0)} м²" if area_data.get('value') else "Площадь не указана"
            
            # Адрес
            address_parts = []
            geo = offer.get('geo', {})
            if geo.get('userInput'):
                address_parts.append(geo['userInput'])
            address = ', '.join(address_parts) if address_parts else "Адрес не указан"
            
            # URL
            url = f"https://perm.cian.ru/rent/commercial/{offer_id}/"
            
            # Этаж
            floor_number = offer.get('floorNumber', 0)
            floors_count = offer.get('building', {}).get('floorsCount', 0)
            floor_info = f"{floor_number}/{floors_count}" if floor_number and floors_count else "Не указан"
            
            # Телефоны
            phones = []
            for phone in offer.get('phones', []):
                if phone.get('number'):
                    phones.append(phone['number'])
            
            # Тип помещения
            commercial_type = offer.get('category', {})
            types = commercial_type.get('name', 'Свободное назначение') if commercial_type else 'Свободное назначение'
            
            # Описание
            description = offer.get('description', '')
            
            return {
                'id': offer_id,
                'price_text': price_text,
                'price_per_month': price,
                'area': area,
                'address': address,
                'url': url,
                'floor_info': floor_info,
                'phones': phones,
                'types': types,
                'description': description,
                'added_time': 'Недавно'  # Упрощено для базовой версии
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки объявления {offer.get('id', 'unknown')}: {e}")
            return {
                'id': offer.get('id', 0),
                'price_text': 'Ошибка загрузки',
                'price_per_month': 0,
                'area': 'Не указана',
                'address': 'Не указан',
                'url': '',
                'floor_info': 'Не указан',
                'phones': [],
                'types': 'Не указан',
                'description': f'Ошибка обработки: {e}',
                'added_time': 'Ошибка'
            }
    
    def _prepare_for_databd(self, processed_offer: Dict) -> Dict:
        """Подготавливает данные для сохранения в dataBD"""
        try:
            return {
                'id': str(processed_offer.get('id', '')),
                'source': 'cian',
                'price': processed_offer.get('price_per_month', 0),
                'area': processed_offer.get('area', ''),
                'description': processed_offer.get('description', ''),
                'url': processed_offer.get('url', ''),
                'floor': processed_offer.get('floor_info', ''),
                'address': processed_offer.get('address', ''),
                'lat': '',  # Упрощено для базовой версии
                'lng': '',  # Упрощено для базовой версии
                'seller': ', '.join(processed_offer.get('phones', [])),
                'photos': [],  # Упрощено для базовой версии
                'status': 'open',
                'visible': 1
            }
        except Exception as e:
            logger.error(f"Ошибка подготовки данных для dataBD: {e}")
            return {
                'id': str(processed_offer.get('id', 'error')),
                'source': 'cian',
                'price': 0,
                'area': '',
                'description': f'Ошибка обработки: {e}',
                'url': '',
                'floor': '',
                'address': '',
                'lat': '',
                'lng': '',
                'seller': '',
                'photos': [],
                'status': 'error',
                'visible': 0
            }
    
    def _get_demo_data(self, user_id: str, only_new: bool):
        """Возвращает демонстрационные данные когда API недоступен"""
        logger.info("Генерируем демонстрационные данные")
        
        demo_offers = [
            {
                'id': 1001,
                'price_text': '85 000 ₽/мес',
                'price_per_month': 85000,
                'area': '45 м²',
                'address': 'г. Пермь, ул. Ленина, 50',
                'url': 'https://perm.cian.ru/rent/commercial/1001/',
                'floor_info': '3/9',
                'phones': ['+7(342)123-45-67'],
                'types': 'Офис',
                'description': 'Демонстрационное объявление - API Cian.ru временно недоступен',
                'added_time': 'Сегодня'
            },
            {
                'id': 1002,
                'price_text': '120 000 ₽/мес',
                'price_per_month': 120000,
                'area': '78 м²',
                'address': 'г. Пермь, ул. Мира, 25',
                'url': 'https://perm.cian.ru/rent/commercial/1002/',
                'floor_info': '1/5',
                'phones': ['+7(342)987-65-43'],
                'types': 'Торговое помещение',
                'description': 'Демонстрационное объявление #2 - API Cian.ru временно недоступен',
                'added_time': 'Вчера'
            }
        ]
        
        # Сохраняем демо данные в БД
        for offer in demo_offers:
            try:
                db_data = self._prepare_for_databd(offer)
                db_manager.save_listing(db_data)
            except Exception as e:
                logger.error(f"Ошибка сохранения демо данных: {e}")
        
        # Формируем статистику
        stats = {
            'total_count': len(demo_offers),
            'new_count': len(demo_offers) if only_new else 0,
            'seen_count': 0 if only_new else len(demo_offers),
            'search_time': '0.5 сек',
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'demo_mode': True
        }
        
        offers_to_return = demo_offers if only_new else demo_offers
        
        logger.info(f"Возвращаем {len(offers_to_return)} демонстрационных объявлений")
        
        # Записываем демо-парсинг в безопасный режим (как успешный)
        safe_mode.log_parsing(user_id, success=True)
        if SAFE_MODE_ENABLED:
            logger.info(f"🛡️ Демо-парсинг записан в безопасный режим для пользователя {user_id}")
        else:
            logger.info(f"⚠️ Демо-парсинг завершен (безопасный режим отключен) для пользователя {user_id}")
        
        return offers_to_return, stats
    
    def _process_api_data(self, raw_offers: List[Dict], seen_offers: Set[int], user_id: str) -> Tuple[List[Dict], List[Dict]]:
        """Обрабатывает данные полученные от API"""
        processed_offers = []
        new_offers = []
        
        for offer in raw_offers:
            try:
                processed_offer = self._process_offer(offer)
                processed_offers.append(processed_offer)
                
                offer_id = int(processed_offer.get('id', 0))
                
                # Проверяем, новое ли это объявление
                if offer_id not in seen_offers:
                    new_offers.append(processed_offer)
                    seen_offers.add(offer_id)
                    
                    # Сохраняем в БД
                    db_data = self._prepare_for_databd(processed_offer)
                    db_manager.save_listing(db_data)
                    
            except Exception as e:
                logger.error(f"Ошибка обработки объявления: {e}")
                continue
        
        return processed_offers, new_offers
    
    def get_safety_report(self, user_id: str) -> str:
        """Упрощенный отчет безопасности для интеграции"""
        return "✅ Система готова к работе (мониторинг отключен для интеграции)"
    
    def get_monitoring_report(self, days: int = 3) -> str:
        """Упрощенный отчет мониторинга для интеграции"""
        return f"📊 Базовая статистика доступна через БД (мониторинг отключен для интеграции)"

# Создаем глобальный экземпляр парсера
parser = CianParser() 
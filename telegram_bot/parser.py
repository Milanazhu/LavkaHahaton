import requests
import json
import os
import time
import random
from datetime import datetime
from typing import List, Dict, Tuple, Set
from config import (
    CIAN_API_URL, HEADERS, DEFAULT_SEARCH_PARAMS, DATA_DIR, SEEN_OFFERS_FILE,
    USER_AGENTS, PROXY_LIST, DELAY_CONFIG, SECURITY_CONFIG
)
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CianParser:
    """Парсер объявлений с Cian.ru с защитой от блокировки"""
    
    def __init__(self):
        self.ensure_data_dir()
        self.session = requests.Session()
        self._setup_session()
        
        # Инициализируем монитор
        try:
            from monitor import monitor, get_current_ip
            self.monitor = monitor
            self.get_current_ip = get_current_ip
            self.monitoring_enabled = True
        except ImportError:
            logger.warning("Модуль мониторинга недоступен")
            self.monitoring_enabled = False
    
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
        
        # Обновление заголовков
        self._update_headers()
    
    def _update_headers(self):
        """Обновляет заголовки с ротацией User-Agent"""
        headers = HEADERS.copy()
        
        if SECURITY_CONFIG['rotate_user_agent'] and USER_AGENTS:
            headers['user-agent'] = random.choice(USER_AGENTS)
            logger.debug(f"User-Agent: {headers['user-agent']}")
        
        self.session.headers.update(headers)
    
    def _apply_delay(self):
        """Применяет случайную задержку для имитации человеческого поведения"""
        if DELAY_CONFIG['enabled']:
            delay = random.uniform(DELAY_CONFIG['min_delay'], DELAY_CONFIG['max_delay'])
            logger.debug(f"Задержка: {delay:.1f} секунд")
            time.sleep(delay)
    
    def _check_safety_before_request(self, user_id: str) -> bool:
        """Проверяет безопасность перед выполнением запроса"""
        if not self.monitoring_enabled:
            return True
        
        safety = self.monitor.check_safety_limits(user_id)
        
        if not safety['safe_to_proceed']:
            warnings_text = "; ".join(safety['warnings'])
            recommendations_text = "; ".join(safety['recommendations'])
            raise Exception(
                f"🚫 Запрос заблокирован системой безопасности\n"
                f"⚠️ Проблемы: {warnings_text}\n"
                f"💡 Рекомендации: {recommendations_text}"
            )
        
        if safety['warnings']:
            for warning in safety['warnings']:
                logger.warning(f"Предупреждение безопасности: {warning}")
        
        return True
    
    def ensure_data_dir(self):
        """Создает директорию для данных если её нет"""
        os.makedirs(DATA_DIR, exist_ok=True)
    
    def load_seen_offers(self, user_id: str = "default") -> Set[int]:
        """Загружает ID уже виденных объявлений для пользователя"""
        try:
            file_path = f"{DATA_DIR}/seen_offers_{user_id}.json"
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    return set(json.load(f))
        except Exception as e:
            logger.error(f"Ошибка загрузки seen_offers для {user_id}: {e}")
        return set()
    
    def save_seen_offers(self, offer_ids: Set[int], user_id: str = "default"):
        """Сохраняет ID объявлений для пользователя"""
        try:
            file_path = f"{DATA_DIR}/seen_offers_{user_id}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(list(offer_ids), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения seen_offers для {user_id}: {e}")
    
    def _make_request_with_retry(self, url: str, json_data: dict) -> requests.Response:
        """Делает запрос с повторами при ошибках"""
        last_exception = None
        
        for attempt in range(SECURITY_CONFIG['max_retries']):
            try:
                # Применяем задержку перед каждым запросом
                if attempt > 0:
                    retry_delay = SECURITY_CONFIG['retry_delay'] + random.uniform(0, 5)
                    logger.info(f"Повтор {attempt + 1}, ожидание {retry_delay:.1f} сек...")
                    time.sleep(retry_delay)
                    
                    # Обновляем заголовки для повтора
                    self._update_headers()
                
                self._apply_delay()
                
                logger.info(f"Попытка {attempt + 1}: отправка запроса к Cian API...")
                response = self.session.post(url, json=json_data)
                
                # Проверяем статус ответа
                if response.status_code == 200:
                    logger.info("Запрос успешно выполнен")
                    return response
                elif response.status_code == 429:
                    logger.warning("Превышен лимит запросов (429), увеличиваем задержку...")
                    time.sleep(30)  # Дополнительная задержка для rate limit
                elif response.status_code == 403:
                    logger.warning("Доступ запрещен (403), возможна блокировка IP")
                    time.sleep(60)  # Длительная задержка при блокировке
                else:
                    logger.warning(f"Неожиданный статус: {response.status_code}")
                
                response.raise_for_status()
                
            except requests.exceptions.Timeout:
                last_exception = requests.RequestException("Таймаут запроса")
                logger.warning(f"Таймаут на попытке {attempt + 1}")
            except requests.exceptions.ConnectionError:
                last_exception = requests.RequestException("Ошибка подключения")
                logger.warning(f"Ошибка подключения на попытке {attempt + 1}")
            except requests.exceptions.RequestException as e:
                last_exception = e
                logger.warning(f"Ошибка запроса на попытке {attempt + 1}: {e}")
        
        # Если все попытки неудачны
        raise last_exception or requests.RequestException("Все попытки запроса неудачны")
    
    def parse_offers(self, user_id: str = "default", only_new: bool = True) -> Tuple[List[Dict], Dict]:
        """
        Парсит объявления с Cian.ru
        
        Args:
            user_id: ID пользователя для отслеживания просмотренных объявлений
            only_new: Возвращать только новые объявления
            
        Returns:
            Tuple[List[Dict], Dict]: (список объявлений, статистика)
        """
        current_ip = None
        
        try:
            # Получаем текущий IP для мониторинга
            if self.monitoring_enabled:
                current_ip = self.get_current_ip()
                logger.info(f"Текущий IP: {current_ip}")
            
            # Проверяем безопасность перед запросом
            self._check_safety_before_request(user_id)
            
            # Загружаем уже виденные объявления
            seen_offers = self.load_seen_offers(user_id) if only_new else set()
            logger.info(f"Загружено {len(seen_offers)} ранее просмотренных объявлений для {user_id}")
            
            # Подготавливаем данные запроса
            json_data = {'jsonQuery': DEFAULT_SEARCH_PARAMS}
            
            # Делаем запрос с повторами
            response = self._make_request_with_retry(CIAN_API_URL, json_data)
            data = response.json()
            
            # Получаем объявления
            offers = data.get('data', {}).get('suggestOffersSerializedList', [])
            if not offers:
                offers = data.get('data', {}).get('offersSerialized', [])
            
            total_count = data.get('data', {}).get('offerCount', 0)
            logger.info(f"Получено {len(offers)} объявлений из {total_count} найденных")
            
            # Фильтруем новые объявления
            new_offers = []
            current_offer_ids = set()
            
            for offer in offers:
                offer_id = offer.get('id')
                if offer_id:
                    current_offer_ids.add(offer_id)
                    if not only_new or offer_id not in seen_offers:
                        new_offers.append(self._process_offer(offer))
            
            # Сохраняем актуальный список ID
            if only_new:
                # Объединяем старые и новые ID
                all_seen = seen_offers.union(current_offer_ids)
                self.save_seen_offers(all_seen, user_id)
                logger.info(f"Сохранено {len(all_seen)} ID объявлений для {user_id}")
            
            # Статистика
            stats = {
                'total_count': total_count,
                'new_count': len(new_offers),
                'seen_count': len(offers) - len(new_offers) if only_new else 0,
                'search_time': datetime.now().strftime('%d.%m.%Y %H:%M:%S')
            }
            
            # Логируем успешный запрос
            if self.monitoring_enabled:
                self.monitor.log_request(
                    user_id=user_id,
                    status='success',
                    offers_count=len(new_offers),
                    ip_info=current_ip
                )
            
            logger.info(f"Парсинг завершен для {user_id}: {len(new_offers)} новых из {total_count}")
            
            return new_offers, stats
            
        except requests.RequestException as e:
            error_msg = f"Ошибка при обращении к Cian.ru: {e}"
            logger.error(error_msg)
            
            # Логируем ошибку
            if self.monitoring_enabled:
                self.monitor.log_request(
                    user_id=user_id,
                    status='error',
                    error_msg=str(e),
                    ip_info=current_ip
                )
            
            # Специальные сообщения для разных типов ошибок
            if "429" in str(e):
                error_msg += "\n💡 Совет: Слишком много запросов. Подождите 1-2 часа."
            elif "403" in str(e):
                error_msg += "\n💡 Совет: Возможна блокировка IP. Попробуйте сменить сеть или использовать VPN."
            elif "timeout" in str(e).lower():
                error_msg += "\n💡 Совет: Проблемы с сетью. Проверьте подключение к интернету."
            
            raise Exception(error_msg)
        except Exception as e:
            logger.error(f"Общая ошибка парсинга: {e}")
            
            # Логируем общую ошибку
            if self.monitoring_enabled:
                self.monitor.log_request(
                    user_id=user_id,
                    status='error',
                    error_msg=str(e),
                    ip_info=current_ip
                )
            
            raise Exception(f"Ошибка обработки данных: {e}")
    
    def get_safety_report(self, user_id: str) -> str:
        """Получает отчет о безопасности для пользователя"""
        if not self.monitoring_enabled:
            return "📊 Система мониторинга недоступна"
        
        safety = self.monitor.check_safety_limits(user_id)
        
        report = ["🛡️ ОТЧЕТ БЕЗОПАСНОСТИ", "=" * 30]
        
        if safety['safe_to_proceed']:
            report.append("✅ Можно выполнять запросы")
        else:
            report.append("🚫 Запросы заблокированы")
        
        if safety['warnings']:
            report.append("\n⚠️ Предупреждения:")
            for warning in safety['warnings']:
                report.append(f"  • {warning}")
        
        if safety['recommendations']:
            report.append("\n💡 Рекомендации:")
            for rec in safety['recommendations']:
                report.append(f"  • {rec}")
        
        # Добавляем краткий отчет активности
        daily_report = self.monitor.get_daily_report(3)
        report.append(f"\n{daily_report}")
        
        return "\n".join(report)
    
    def _process_offer(self, offer: Dict) -> Dict:
        """Обрабатывает одно объявление, извлекая нужные поля"""
        try:
            # Основная информация
            offer_id = offer.get('id', 'Не указан')
            
            # Получаем числовое значение площади (нужно для расчета цены)
            area_numeric = 0
            area_text = offer.get('totalArea', 'Не указана')
            if area_text and area_text != 'Не указана':
                try:
                    area_numeric = float(area_text)
                except:
                    pass
            
            # Цена
            price_info = offer.get('bargainTerms', {})
            if price_info.get('price'):
                price = price_info['price']
                
                if price_info.get('priceType') == 'squareMeter' and area_numeric > 0:
                    # Цена за м² - умножаем на площадь для получения общей цены
                    total_monthly_price = price * area_numeric
                    price_text = f"{total_monthly_price:,.0f} ₽/мес."
                    price_per_month = total_monthly_price
                else:
                    # Цена уже общая за месяц
                    price_text = f"{price:,} ₽/мес."
                    price_per_month = price
            else:
                price_text = offer.get('formattedShortPrice', 'Не указана')
                price_per_month = 0
            
            # Площадь
            area = offer.get('totalArea', 'Не указана')
            if area and area != 'Не указана':
                area = f"{area} м²"
            
            # Адрес
            geo = offer.get('geo', {})
            address = geo.get('userInput', 'Не указан')
            
            # Координаты
            coordinates = geo.get('coordinates', {})
            lat = coordinates.get('lat', 0)
            lng = coordinates.get('lng', 0)
            
            # Этаж
            floor = offer.get('floorNumber', 'Не указан')
            building = offer.get('building', {})
            floors_total = building.get('floorsCount', 'Не указано')
            floor_info = f"{floor}/{floors_total}"
            
            # Тип помещения
            specialty = offer.get('specialty', {})
            specialties = specialty.get('specialties', [])
            types_ru = []
            for spec in specialties[:5]:  # Берем первые 5 типов
                if spec.get('rusName'):
                    types_ru.append(spec['rusName'])
            
            if types_ru:
                types_text = ', '.join(types_ru)
                if len(specialties) > 5:
                    types_text += f" и еще {len(specialties) - 5}"
            else:
                types_text = "Свободное назначение"
            
            # Описание
            description = offer.get('description', '')
            
            # Контакты
            phones = offer.get('phones', [])
            phone_numbers = []
            for phone in phones:
                country_code = phone.get('countryCode', '7')
                number = phone.get('number', '')
                if number:
                    phone_numbers.append(f"+{country_code} {number}")
            
            # Ссылка
            full_url = offer.get('fullUrl', '')
            
            # Время добавления
            added_time = offer.get('humanizedTimedelta', 'Не указано')
            
            # Фотографии
            photos = offer.get('photos', [])
            photo_urls = []
            for photo in photos[:3]:  # Берем первые 3 фото
                if photo.get('fullUrl'):
                    photo_urls.append(photo['fullUrl'])
            
            return {
                'id': offer_id,
                'price_text': price_text,
                'price_per_month': price_per_month,
                'area': area,
                'area_numeric': area_numeric,
                'address': address,
                'coordinates': {'lat': lat, 'lng': lng},
                'floor_info': floor_info,
                'floor': floor,
                'floors_total': floors_total,
                'types': types_text,
                'description': description,
                'phones': phone_numbers,
                'url': full_url,
                'added_time': added_time,
                'photos': photo_urls,
                'raw_data': offer  # Сохраняем исходные данные на случай нужды
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки объявления {offer.get('id', 'unknown')}: {e}")
            return {
                'id': offer.get('id', 'Ошибка'),
                'price_text': 'Ошибка обработки',
                'error': str(e)
            }

# Создаем глобальный экземпляр парсера
parser = CianParser() 
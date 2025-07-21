#!/usr/bin/env python3
"""
Система мониторинга активности парсера
Отслеживает запросы, ошибки и помогает выявлять блокировки
"""

import logging
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from config import DATA_DIR

# Настройка логирования
LOG_FILE = f"{DATA_DIR}/parser_activity.log"
STATS_FILE = f"{DATA_DIR}/monitor_stats.json"

class ParserMonitor:
    """Монитор активности парсера для предотвращения блокировки"""
    
    def __init__(self):
        self.ensure_data_dir()
        self.setup_logging()
        self.stats = self.load_stats()
    
    def ensure_data_dir(self):
        """Создает директорию для данных если её нет"""
        os.makedirs(DATA_DIR, exist_ok=True)
    
    def setup_logging(self):
        """Настраивает логирование активности"""
        # Создаем отдельный логгер для монитора
        self.logger = logging.getLogger('parser_monitor')
        self.logger.setLevel(logging.INFO)
        
        # Проверяем, есть ли уже обработчики
        if not self.logger.handlers:
            # Файловый обработчик
            file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            
            # Формат логов
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
    
    def load_stats(self) -> Dict:
        """Загружает статистику из файла"""
        try:
            if os.path.exists(STATS_FILE):
                with open(STATS_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"Ошибка загрузки статистики: {e}")
        
        return {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'last_request_time': None,
            'daily_requests': {},
            'error_history': [],
            'user_activity': {}
        }
    
    def save_stats(self):
        """Сохраняет статистику в файл"""
        try:
            with open(STATS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.stats, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Ошибка сохранения статистики: {e}")
    
    def log_request(self, user_id: str, status: str, offers_count: int = 0, 
                   error_msg: str = None, ip_info: str = None):
        """
        Логирует запрос к API
        
        Args:
            user_id: ID пользователя
            status: Статус запроса (success/error)
            offers_count: Количество полученных объявлений
            error_msg: Сообщение об ошибке (если есть)
            ip_info: Информация об IP (если доступна)
        """
        timestamp = datetime.now()
        date_key = timestamp.strftime('%Y-%m-%d')
        
        # Обновляем общую статистику
        self.stats['total_requests'] += 1
        self.stats['last_request_time'] = timestamp.isoformat()
        
        if status == 'success':
            self.stats['successful_requests'] += 1
        else:
            self.stats['failed_requests'] += 1
            
            # Сохраняем информацию об ошибке
            error_info = {
                'timestamp': timestamp.isoformat(),
                'user_id': user_id,
                'error': error_msg,
                'ip_info': ip_info
            }
            self.stats['error_history'].append(error_info)
            
            # Оставляем только последние 50 ошибок
            if len(self.stats['error_history']) > 50:
                self.stats['error_history'] = self.stats['error_history'][-50:]
        
        # Дневная статистика
        if date_key not in self.stats['daily_requests']:
            self.stats['daily_requests'][date_key] = {
                'total': 0,
                'success': 0,
                'errors': 0,
                'users': set()
            }
        
        daily = self.stats['daily_requests'][date_key]
        daily['total'] += 1
        daily['users'].add(user_id)
        
        if status == 'success':
            daily['success'] += 1
        else:
            daily['errors'] += 1
        
        # Конвертируем set в list для JSON
        daily['users'] = list(daily['users'])
        
        # Активность пользователей
        if user_id not in self.stats['user_activity']:
            self.stats['user_activity'][user_id] = {
                'total_requests': 0,
                'last_request': None,
                'offers_received': 0
            }
        
        user_stats = self.stats['user_activity'][user_id]
        user_stats['total_requests'] += 1
        user_stats['last_request'] = timestamp.isoformat()
        if offers_count > 0:
            user_stats['offers_received'] += offers_count
        
        # Логируем в файл
        log_msg = f"USER:{user_id} STATUS:{status} OFFERS:{offers_count}"
        if ip_info:
            log_msg += f" IP:{ip_info}"
        if error_msg:
            log_msg += f" ERROR:{error_msg}"
        
        if status == 'success':
            self.logger.info(log_msg)
        else:
            self.logger.error(log_msg)
        
        # Сохраняем статистику
        self.save_stats()
    
    def check_safety_limits(self, user_id: str) -> Dict[str, any]:
        """
        Проверяет безопасные лимиты использования
        
        Returns:
            Dict с рекомендациями по безопасности
        """
        now = datetime.now()
        today = now.strftime('%Y-%m-%d')
        
        result = {
            'safe_to_proceed': True,
            'warnings': [],
            'recommendations': []
        }
        
        # Проверяем дневные лимиты
        daily_stats = self.stats['daily_requests'].get(today, {'total': 0})
        if daily_stats['total'] >= 10:
            result['safe_to_proceed'] = False
            result['warnings'].append(f"Превышен дневной лимит: {daily_stats['total']} запросов")
            result['recommendations'].append("Сделайте перерыв до завтра")
        elif daily_stats['total'] >= 5:
            result['warnings'].append(f"Приближение к дневному лимиту: {daily_stats['total']}/10")
            result['recommendations'].append("Ограничьте количество запросов")
        
        # Проверяем время последнего запроса
        if self.stats['last_request_time']:
            last_request = datetime.fromisoformat(self.stats['last_request_time'])
            time_diff = now - last_request
            
            if time_diff < timedelta(hours=2):
                result['warnings'].append(f"Последний запрос был {time_diff} назад")
                result['recommendations'].append("Рекомендуется интервал минимум 3 часа")
            
            if time_diff < timedelta(hours=1):
                result['safe_to_proceed'] = False
                result['warnings'].append("Слишком частые запросы!")
                result['recommendations'].append("Обязательно подождите минимум 2 часа")
        
        # Проверяем количество ошибок
        recent_errors = [
            err for err in self.stats['error_history']
            if (now - datetime.fromisoformat(err['timestamp'])) < timedelta(hours=24)
        ]
        
        if len(recent_errors) >= 3:
            result['safe_to_proceed'] = False
            result['warnings'].append(f"Много ошибок за последние 24 часа: {len(recent_errors)}")
            result['recommendations'].append("Возможна блокировка IP. Смените сеть или используйте VPN")
        
        # Проверяем активность пользователя
        user_stats = self.stats['user_activity'].get(user_id)
        if user_stats and user_stats['last_request']:
            last_user_request = datetime.fromisoformat(user_stats['last_request'])
            user_time_diff = now - last_user_request
            
            if user_time_diff < timedelta(hours=3):
                result['recommendations'].append(f"Ваш последний запрос был {user_time_diff} назад")
        
        return result
    
    def get_daily_report(self, days: int = 7) -> str:
        """Генерирует отчет за несколько дней"""
        now = datetime.now()
        report_lines = ["📊 ОТЧЕТ АКТИВНОСТИ ПАРСЕРА", "=" * 40]
        
        total_requests = 0
        total_errors = 0
        
        for i in range(days):
            date = (now - timedelta(days=i)).strftime('%Y-%m-%d')
            daily = self.stats['daily_requests'].get(date, {})
            
            if daily:
                requests_count = daily.get('total', 0)
                errors_count = daily.get('errors', 0)
                users_count = len(daily.get('users', []))
                
                total_requests += requests_count
                total_errors += errors_count
                
                status = "🔴" if errors_count > 2 else "🟡" if requests_count > 5 else "🟢"
                
                report_lines.append(
                    f"{status} {date}: {requests_count} запросов, "
                    f"{errors_count} ошибок, {users_count} пользователей"
                )
            else:
                report_lines.append(f"⚪ {date}: нет активности")
        
        report_lines.extend([
            "",
            f"📈 Итого за {days} дней:",
            f"   Запросов: {total_requests}",
            f"   Ошибок: {total_errors}",
            f"   Успешность: {((total_requests - total_errors) / max(total_requests, 1) * 100):.1f}%"
        ])
        
        # Последние ошибки
        if self.stats['error_history']:
            report_lines.extend(["", "🚨 Последние ошибки:"])
            for error in self.stats['error_history'][-3:]:
                timestamp = datetime.fromisoformat(error['timestamp']).strftime('%d.%m %H:%M')
                report_lines.append(f"   {timestamp}: {error.get('error', 'Неизвестная ошибка')}")
        
        return "\n".join(report_lines)
    
    def cleanup_old_logs(self, days_to_keep: int = 30):
        """Очищает старые записи"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d')
        
        # Очищаем дневную статистику
        old_dates = [
            date for date in self.stats['daily_requests'].keys()
            if date < cutoff_str
        ]
        
        for date in old_dates:
            del self.stats['daily_requests'][date]
        
        # Очищаем историю ошибок
        self.stats['error_history'] = [
            error for error in self.stats['error_history']
            if datetime.fromisoformat(error['timestamp']) > cutoff_date
        ]
        
        self.save_stats()
        self.logger.info(f"Очищены данные старше {days_to_keep} дней")

# Создаем глобальный экземпляр монитора
monitor = ParserMonitor()

def get_current_ip() -> Optional[str]:
    """Получает текущий IP адрес"""
    try:
        import requests
        response = requests.get('https://api.ipify.org', timeout=5)
        return response.text.strip()
    except:
        return None

if __name__ == "__main__":
    # Тестовый запуск
    print("🔍 Тест системы мониторинга")
    
    # Пример логирования
    current_ip = get_current_ip()
    monitor.log_request("test_user", "success", 5, ip_info=current_ip)
    
    # Проверка лимитов
    safety = monitor.check_safety_limits("test_user")
    print(f"Безопасно: {safety['safe_to_proceed']}")
    if safety['warnings']:
        print("Предупреждения:", safety['warnings'])
    if safety['recommendations']:
        print("Рекомендации:", safety['recommendations'])
    
    # Отчет
    print("\n" + monitor.get_daily_report()) 
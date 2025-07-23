#!/usr/bin/env python3
"""
Модуль безопасного режима для парсинга
Ограничивает парсинг одним разом в сутки для предотвращения блокировки
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional
import logging
from config import SAFE_MODE_ENABLED

logger = logging.getLogger(__name__)

class SafeMode:
    """Класс для управления безопасным режимом парсинга"""
    
    def __init__(self, db_path: str = "dataBD/real_estate_data.db"):
        self.db_path = db_path
        self.safety_interval_hours = 24  # 24 часа между парсингами
        self.enabled = SAFE_MODE_ENABLED  # Управляется флагом в config.py
        if self.enabled:
            self.init_safety_table()
            logger.info("🛡️ Безопасный режим включен")
        else:
            logger.info("⚠️ Безопасный режим отключен")
    
    def init_safety_table(self):
        """Создает таблицу для отслеживания безопасного режима"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Создаем таблицу для отслеживания последних парсингов
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS safety_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        last_parsing_time TIMESTAMP,
                        parsing_count_today INTEGER DEFAULT 0,
                        total_parsing_count INTEGER DEFAULT 0,
                        last_reset_date TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Создаем индекс для быстрого поиска по пользователю
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_safety_user_id 
                    ON safety_log(user_id)
                """)
                
                conn.commit()
                logger.info("Таблица безопасного режима инициализирована")
                
        except Exception as e:
            logger.error(f"Ошибка инициализации таблицы безопасности: {e}")
            raise
    
    def can_parse(self, user_id: str) -> Tuple[bool, Dict]:
        """
        Проверяет, можно ли запустить парсинг для пользователя
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Tuple[bool, Dict]: (разрешен ли парсинг, информация о состоянии)
        """
        # Если безопасный режим отключен - всегда разрешаем парсинг
        if not self.enabled:
            return True, {
                'status': 'disabled',
                'message': '✅ Безопасный режим отключен - парсинг всегда доступен',
                'total_today': 0,
                'total_all_time': 0,
                'mode': 'disabled'
            }
        
        try:
            current_time = datetime.now()
            current_date = current_time.strftime('%Y-%m-%d')
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Получаем данные о последнем парсинге пользователя
                cursor.execute("""
                    SELECT last_parsing_time, parsing_count_today, last_reset_date, total_parsing_count
                    FROM safety_log 
                    WHERE user_id = ?
                """, (user_id,))
                
                result = cursor.fetchone()
                
                if not result:
                    # Первый парсинг пользователя
                    return True, {
                        'status': 'first_time',
                        'message': '✅ Первый запуск парсинга разрешен',
                        'next_available': 'Через 24 часа после этого парсинга',
                        'total_today': 0,
                        'total_all_time': 0
                    }
                
                last_parsing_str, count_today, last_reset_date, total_count = result
                
                # Сброс счетчика если сменился день
                if last_reset_date != current_date:
                    count_today = 0
                
                # Если уже парсили сегодня
                if count_today > 0:
                    last_parsing = datetime.fromisoformat(last_parsing_str)
                    time_since_last = current_time - last_parsing
                    time_left = timedelta(hours=self.safety_interval_hours) - time_since_last
                    
                    if time_left.total_seconds() > 0:
                        # Парсинг заблокирован
                        hours_left = int(time_left.total_seconds() // 3600)
                        minutes_left = int((time_left.total_seconds() % 3600) // 60)
                        
                        next_available_time = last_parsing + timedelta(hours=self.safety_interval_hours)
                        
                        return False, {
                            'status': 'blocked',
                            'message': f'🚫 Парсинг заблокирован безопасным режимом',
                            'hours_left': hours_left,
                            'minutes_left': minutes_left,
                            'next_available': next_available_time.strftime('%d.%m.%Y в %H:%M'),
                            'last_parsing': last_parsing.strftime('%d.%m.%Y в %H:%M'),
                            'total_today': count_today,
                            'total_all_time': total_count or 0
                        }
                
                # Парсинг разрешен
                return True, {
                    'status': 'allowed',
                    'message': '✅ Парсинг разрешен безопасным режимом',
                    'last_parsing': last_parsing_str if last_parsing_str else 'Никогда',
                    'total_today': count_today,
                    'total_all_time': total_count or 0
                }
                
        except Exception as e:
            logger.error(f"Ошибка проверки безопасного режима для {user_id}: {e}")
            # В случае ошибки разрешаем парсинг
            return True, {
                'status': 'error',
                'message': '⚠️ Ошибка проверки безопасности, парсинг разрешен',
                'error': str(e)
            }
    
    def log_parsing(self, user_id: str, success: bool = True) -> bool:
        """
        Записывает информацию о выполненном парсинге
        
        Args:
            user_id: ID пользователя
            success: Успешность парсинга
            
        Returns:
            bool: Успешность записи
        """
        # Если безопасный режим отключен - не записываем статистику
        if not self.enabled:
            logger.info(f"Безопасный режим отключен - статистика парсинга для {user_id} не записывается")
            return True
        
        try:
            current_time = datetime.now()
            current_date = current_time.strftime('%Y-%m-%d')
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Проверяем, есть ли запись для пользователя
                cursor.execute("""
                    SELECT id, parsing_count_today, total_parsing_count, last_reset_date
                    FROM safety_log 
                    WHERE user_id = ?
                """, (user_id,))
                
                result = cursor.fetchone()
                
                if result:
                    # Обновляем существующую запись
                    record_id, count_today, total_count, last_reset_date = result
                    
                    # Сброс счетчика если сменился день
                    if last_reset_date != current_date:
                        count_today = 0
                    
                    new_count_today = count_today + 1 if success else count_today
                    new_total_count = (total_count or 0) + 1 if success else (total_count or 0)
                    
                    cursor.execute("""
                        UPDATE safety_log 
                        SET last_parsing_time = ?,
                            parsing_count_today = ?,
                            total_parsing_count = ?,
                            last_reset_date = ?,
                            updated_at = ?
                        WHERE id = ?
                    """, (
                        current_time.isoformat(),
                        new_count_today,
                        new_total_count,
                        current_date,
                        current_time.isoformat(),
                        record_id
                    ))
                else:
                    # Создаем новую запись
                    cursor.execute("""
                        INSERT INTO safety_log 
                        (user_id, last_parsing_time, parsing_count_today, total_parsing_count, last_reset_date)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        user_id,
                        current_time.isoformat(),
                        1 if success else 0,
                        1 if success else 0,
                        current_date
                    ))
                
                conn.commit()
                
                if success:
                    logger.info(f"Записан успешный парсинг для пользователя {user_id}")
                else:
                    logger.info(f"Записан неуспешный парсинг для пользователя {user_id}")
                
                return True
                
        except Exception as e:
            logger.error(f"Ошибка записи парсинга для {user_id}: {e}")
            return False
    
    def get_user_safety_stats(self, user_id: str) -> Dict:
        """
        Получает статистику безопасного режима для пользователя
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Dict: Статистика безопасности
        """
        # Если безопасный режим отключен
        if not self.enabled:
            return {
                'user_id': user_id,
                'mode': 'disabled',
                'first_parsing': 'Безопасный режим отключен',
                'last_parsing': 'Статистика не ведется',
                'today_count': 0,
                'total_count': 0,
                'can_parse_now': True,
                'status': 'disabled',
                'next_available': 'Всегда доступно',
                'safety_interval': 'Отключен'
            }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT last_parsing_time, parsing_count_today, total_parsing_count, 
                           last_reset_date, created_at
                    FROM safety_log 
                    WHERE user_id = ?
                """, (user_id,))
                
                result = cursor.fetchone()
                
                if not result:
                    return {
                        'user_id': user_id,
                        'first_parsing': 'Парсинг еще не выполнялся',
                        'last_parsing': 'Никогда',
                        'today_count': 0,
                        'total_count': 0,
                        'status': 'ready'
                    }
                
                last_parsing_str, today_count, total_count, last_reset, created_at = result
                
                # Проверяем текущий статус
                can_parse, status_info = self.can_parse(user_id)
                
                return {
                    'user_id': user_id,
                    'first_parsing': created_at,
                    'last_parsing': last_parsing_str,
                    'today_count': today_count or 0,
                    'total_count': total_count or 0,
                    'can_parse_now': can_parse,
                    'status': status_info.get('status', 'unknown'),
                    'next_available': status_info.get('next_available', 'Доступно сейчас'),
                    'safety_interval': f"{self.safety_interval_hours} часов"
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики безопасности для {user_id}: {e}")
            return {
                'user_id': user_id,
                'error': str(e),
                'status': 'error'
            }
    
    def get_global_safety_stats(self) -> Dict:
        """
        Получает глобальную статистику безопасного режима
        
        Returns:
            Dict: Глобальная статистика
        """
        # Если безопасный режим отключен
        if not self.enabled:
            return {
                'total_users': 0,
                'total_parsings': 0,
                'today_parsings': 0,
                'last_parsing': 'Статистика не ведется',
                'safety_interval': 'Отключен',
                'system_status': '⚠️ Безопасный режим отключен'
            }
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Общее количество пользователей
                cursor.execute("SELECT COUNT(DISTINCT user_id) FROM safety_log")
                total_users = cursor.fetchone()[0] or 0
                
                # Общее количество парсингов
                cursor.execute("SELECT SUM(total_parsing_count) FROM safety_log")
                total_parsings = cursor.fetchone()[0] or 0
                
                # Парсинги сегодня
                current_date = datetime.now().strftime('%Y-%m-%d')
                cursor.execute("""
                    SELECT SUM(parsing_count_today) 
                    FROM safety_log 
                    WHERE last_reset_date = ?
                """, (current_date,))
                today_parsings = cursor.fetchone()[0] or 0
                
                # Последний парсинг
                cursor.execute("""
                    SELECT user_id, last_parsing_time 
                    FROM safety_log 
                    WHERE last_parsing_time IS NOT NULL
                    ORDER BY last_parsing_time DESC 
                    LIMIT 1
                """)
                last_parsing_result = cursor.fetchone()
                
                last_parsing_info = "Никогда"
                if last_parsing_result:
                    last_user, last_time = last_parsing_result
                    last_parsing_info = f"Пользователь {last_user} в {last_time}"
                
                return {
                    'total_users': total_users,
                    'total_parsings': total_parsings,
                    'today_parsings': today_parsings,
                    'last_parsing': last_parsing_info,
                    'safety_interval': f"{self.safety_interval_hours} часов",
                    'system_status': '🛡️ Безопасный режим активен'
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения глобальной статистики безопасности: {e}")
            return {
                'error': str(e),
                'system_status': '❌ Ошибка безопасного режима'
            }
    
    def emergency_reset(self, user_id: str) -> bool:
        """
        Экстренный сброс ограничений для пользователя (только для админов)
        
        Args:
            user_id: ID пользователя
            
        Returns:
            bool: Успешность сброса
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    UPDATE safety_log 
                    SET last_parsing_time = NULL,
                        parsing_count_today = 0,
                        updated_at = ?
                    WHERE user_id = ?
                """, (datetime.now().isoformat(), user_id))
                
                conn.commit()
                
                logger.warning(f"Выполнен экстренный сброс ограничений для пользователя {user_id}")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка экстренного сброса для {user_id}: {e}")
            return False

# Создаем глобальный экземпляр безопасного режима
safe_mode = SafeMode("dataBD/real_estate_data.db") 
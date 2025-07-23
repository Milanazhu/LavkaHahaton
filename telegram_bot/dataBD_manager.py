#!/usr/bin/env python3
"""
Менеджер базы данных для папки dataBD
Отдельная база данных для хранения всех спаршенных объявлений
"""

import sqlite3
import json
import os
from datetime import datetime
from typing import List, Dict, Optional, Any
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataBDManager:
    """Менеджер базы данных для папки dataBD"""
    
    def __init__(self, db_path: str = "dataBD/real_estate_data.db"):
        """
        Инициализация менеджера БД
        
        Args:
            db_path: Путь к файлу базы данных
        """
        # Создаем папку если её нет
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Инициализация базы данных и создание таблиц"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA foreign_keys = ON")
                cursor = conn.cursor()
                
                # Создаем таблицу для объявлений
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS real_estate_listings (
                        id TEXT PRIMARY KEY,
                        source TEXT NOT NULL,
                        price REAL,
                        area TEXT,
                        description TEXT,
                        url TEXT,
                        floor TEXT,
                        address TEXT,
                        lat TEXT,
                        lng TEXT,
                        seller TEXT,
                        photos TEXT,
                        status TEXT DEFAULT 'open',
                        visible INTEGER DEFAULT 1
                    )
                """)
                
                # Создаем индексы для оптимизации поиска
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_listings_source ON real_estate_listings(source)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_listings_price ON real_estate_listings(price)
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_listings_coordinates ON real_estate_listings(lat, lng)
                """)
                
                # Создаем таблицу для сессий парсинга
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS parsing_sessions (
                        session_id TEXT PRIMARY KEY,
                        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        finished_at TIMESTAMP,
                        total_parsed INTEGER DEFAULT 0,
                        total_saved INTEGER DEFAULT 0,
                        source TEXT,
                        status TEXT DEFAULT 'running',
                        notes TEXT
                    )
                """)
                
                # Создаем таблицу для статистики
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS daily_stats (
                        date TEXT PRIMARY KEY,
                        total_listings INTEGER DEFAULT 0,
                        new_listings INTEGER DEFAULT 0,
                        updated_listings INTEGER DEFAULT 0,
                        avg_price REAL,
                        min_price REAL,
                        max_price REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                conn.commit()
                logger.info(f"База данных инициализирована: {self.db_path}")
                
        except Exception as e:
            logger.error(f"Ошибка инициализации БД: {e}")
            raise
    
    def start_parsing_session(self, source: str = "cian", notes: str = "") -> str:
        """
        Начинает новую сессию парсинга
        
        Args:
            source: Источник данных
            notes: Заметки о сессии
        
        Returns:
            str: ID сессии
        """
        session_id = f"{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO parsing_sessions (session_id, source, notes)
                    VALUES (?, ?, ?)
                """, (session_id, source, notes))
                conn.commit()
                
                logger.info(f"Начата сессия парсинга: {session_id}")
                return session_id
                
        except Exception as e:
            logger.error(f"Ошибка создания сессии парсинга: {e}")
            raise
    
    def finish_parsing_session(self, session_id: str, total_parsed: int, total_saved: int):
        """
        Завершает сессию парсинга
        
        Args:
            session_id: ID сессии
            total_parsed: Общее количество обработанных объявлений
            total_saved: Количество сохраненных объявлений
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE parsing_sessions 
                    SET finished_at = CURRENT_TIMESTAMP, 
                        total_parsed = ?, 
                        total_saved = ?,
                        status = 'completed'
                    WHERE session_id = ?
                """, (total_parsed, total_saved, session_id))
                conn.commit()
                
                logger.info(f"Завершена сессия парсинга: {session_id} ({total_saved}/{total_parsed})")
                
        except Exception as e:
            logger.error(f"Ошибка завершения сессии парсинга: {e}")
    
    def save_listing(self, listing_data: Dict[str, Any]) -> bool:
        """
        Сохраняет объявление в базу данных
        
        Args:
            listing_data: Данные объявления
        
        Returns:
            bool: True если сохранено, False если обновлено
        """
        try:
            # Конвертируем photos в JSON строку если это список
            photos = listing_data.get('photos', [])
            if isinstance(photos, list):
                photos_json = json.dumps(photos, ensure_ascii=False)
            else:
                photos_json = str(photos)
            

            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Проверяем существует ли объявление
                cursor.execute("SELECT id FROM real_estate_listings WHERE id = ?", (listing_data['id'],))
                exists = cursor.fetchone()
                
                if exists:
                    # Обновляем существующее объявление
                    cursor.execute("""
                        UPDATE real_estate_listings SET
                            source = ?,
                            price = ?,
                            area = ?,
                            description = ?,
                            url = ?,
                            floor = ?,
                            address = ?,
                            lat = ?,
                            lng = ?,
                            seller = ?,
                            photos = ?,
                            status = ?,
                            visible = ?
                        WHERE id = ?
                    """, (
                        listing_data.get('source', 'cian'),
                        listing_data.get('price'),
                        listing_data.get('area'),
                        listing_data.get('description'),
                        listing_data.get('url'),
                        listing_data.get('floor'),
                        listing_data.get('address'),
                        listing_data.get('lat'),
                        listing_data.get('lng'),
                        listing_data.get('seller'),
                        photos_json,
                        listing_data.get('status', 'open'),
                        listing_data.get('visible', 1),
                        listing_data['id']
                    ))
                    
                    conn.commit()
                    logger.debug(f"Обновлено объявление: {listing_data['id']}")
                    return False
                    
                else:
                    # Вставляем новое объявление
                    cursor.execute("""
                        INSERT INTO real_estate_listings (
                            id, source, price, area, description, url, floor, address,
                            lat, lng, seller, photos, status, visible
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        listing_data['id'],
                        listing_data.get('source', 'cian'),
                        listing_data.get('price'),
                        listing_data.get('area'),
                        listing_data.get('description'),
                        listing_data.get('url'),
                        listing_data.get('floor'),
                        listing_data.get('address'),
                        listing_data.get('lat'),
                        listing_data.get('lng'),
                        listing_data.get('seller'),
                        photos_json,
                        listing_data.get('status', 'open'),
                        listing_data.get('visible', 1)
                    ))
                    
                    conn.commit()
                    logger.debug(f"Сохранено новое объявление: {listing_data['id']}")
                    return True
                    
        except Exception as e:
            logger.error(f"Ошибка сохранения объявления {listing_data.get('id')}: {e}")
            return False
    
    def get_listings(self, limit: int = 100, offset: int = 0, source: str = None) -> List[Dict]:
        """
        Получает список объявлений
        
        Args:
            limit: Лимит количества записей
            offset: Смещение
            source: Фильтр по источнику
        
        Returns:
            List[Dict]: Список объявлений
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                query = "SELECT * FROM real_estate_listings"
                params = []
                
                if source:
                    query += " WHERE source = ?"
                    params.append(source)
                
                query += " ORDER BY id DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])
                
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка получения объявлений: {e}")
            return []
    
    def get_listing_by_id(self, listing_id: str) -> Optional[Dict]:
        """
        Получает объявление по ID
        
        Args:
            listing_id: ID объявления
        
        Returns:
            Optional[Dict]: Данные объявления или None
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("SELECT * FROM real_estate_listings WHERE id = ?", (listing_id,))
                row = cursor.fetchone()
                
                return dict(row) if row else None
                
        except Exception as e:
            logger.error(f"Ошибка получения объявления {listing_id}: {e}")
            return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Получает общую статистику по базе данных
        
        Returns:
            Dict[str, Any]: Статистика
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Общая статистика
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_listings,
                        COUNT(CASE WHEN visible = 1 THEN 1 END) as visible_listings,
                        AVG(CASE WHEN price > 0 THEN price END) as avg_price,
                        MIN(CASE WHEN price > 0 THEN price END) as min_price,
                        MAX(CASE WHEN price > 0 THEN price END) as max_price,
                        COUNT(DISTINCT source) as sources_count
                    FROM real_estate_listings
                """)
                
                stats_row = cursor.fetchone()
                stats = {
                    'total_listings': stats_row[0],
                    'visible_listings': stats_row[1], 
                    'avg_price': stats_row[2],
                    'min_price': stats_row[3],
                    'max_price': stats_row[4],
                    'sources_count': stats_row[5]
                }
                
                # Статистика по источникам
                cursor.execute("""
                    SELECT source, COUNT(*) as count
                    FROM real_estate_listings
                    GROUP BY source
                """)
                
                sources = {row[0]: row[1] for row in cursor.fetchall()}
                stats['by_source'] = sources
                
                # Статистика по сессиям
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_sessions,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_sessions,
                        COUNT(CASE WHEN status = 'running' THEN 1 END) as running_sessions
                    FROM parsing_sessions
                """)
                
                sessions_row = cursor.fetchone()
                sessions_stats = {
                    'total_sessions': sessions_row[0],
                    'completed_sessions': sessions_row[1],
                    'running_sessions': sessions_row[2]
                }
                stats['sessions'] = sessions_stats
                
                return stats
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}
    
    def update_daily_stats(self):
        """Обновляет ежедневную статистику"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Получаем статистику за сегодня
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_today,
                        COUNT(*) as new_today,
                        0 as updated_today,
                        AVG(CASE WHEN price > 0 THEN price END) as avg_price,
                        MIN(CASE WHEN price > 0 THEN price END) as min_price,
                        MAX(CASE WHEN price > 0 THEN price END) as max_price
                    FROM real_estate_listings
                    WHERE visible = 1
                """)
                
                stats = cursor.fetchone()
                
                # Обновляем или вставляем статистику
                cursor.execute("""
                    INSERT OR REPLACE INTO daily_stats 
                    (date, total_listings, new_listings, updated_listings, avg_price, min_price, max_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (today, stats[0], stats[1], stats[2], stats[3], stats[4], stats[5]))
                
                conn.commit()
                logger.info(f"Обновлена ежедневная статистика за {today}")
                
        except Exception as e:
            logger.error(f"Ошибка обновления ежедневной статистики: {e}")
    
    def cleanup_old_listings(self, days: int = 30):
        """
        Удаляет старые объявления (упрощенная версия)
        
        Args:
            days: Количество дней для хранения (не используется в упрощенной схеме)
        """
        try:
            logger.info(f"Метод cleanup_old_listings отключен для упрощенной схемы БД")
            return 0
                
        except Exception as e:
            logger.error(f"Ошибка очистки старых объявлений: {e}")
            return 0
    
    # Методы для совместимости с database.py
    
    def save_offer(self, offer_data: Dict) -> bool:
        """
        Совместимость с database.py - сохраняет объявление
        
        Args:
            offer_data: Данные объявления
        
        Returns:
            bool: True если новое объявление
        """
        # Преобразуем формат данных если нужно
        listing_data = {
            'id': str(offer_data.get('id', '')),
            'source': 'cian',
            'price': offer_data.get('price_per_month', 0),
            'area': offer_data.get('area', ''),
            'description': offer_data.get('description', ''),
            'url': offer_data.get('url', ''),
            'floor': offer_data.get('floor', ''),
            'address': offer_data.get('address', ''),
            'lat': str(offer_data.get('coordinates', {}).get('lat', '')),
            'lng': str(offer_data.get('coordinates', {}).get('lng', '')),
            'seller': str(offer_data.get('phones', [])),
            'photos': offer_data.get('photos', []),
            'status': 'open',
            'visible': 1
        }
        
        return self.save_listing(listing_data)
    
    def get_seen_offers(self, user_id: str) -> set:
        """
        Возвращает множество просмотренных объявлений для пользователя
        
        Args:
            user_id: ID пользователя
        
        Returns:
            set: Множество ID просмотренных объявлений
        """
        # Для упрощения возвращаем пустое множество
        # В полной версии можно добавить таблицу user_seen_offers
        return set()
    
    def mark_offers_as_seen_bulk(self, user_id: str, offer_ids: list):
        """
        Отмечает объявления как просмотренные пользователем
        
        Args:
            user_id: ID пользователя  
            offer_ids: Список ID объявлений
        """
        # Для упрощения ничего не делаем
        # В полной версии можно добавить таблицу user_seen_offers
        logger.debug(f"Отмечено {len(offer_ids)} объявлений как просмотренные для {user_id}")
        pass
    
    def save_user(self, user_id: str, username: str = None, first_name: str = None, 
                  last_name: str = None, is_bot: bool = False):
        """
        Сохраняет информацию о пользователе
        
        Args:
            user_id: ID пользователя
            username: Имя пользователя
            first_name: Имя
            last_name: Фамилия  
            is_bot: Является ли ботом
        """
        # Для упрощения ничего не делаем
        # В полной версии можно добавить таблицу users
        logger.debug(f"Сохранен пользователь {user_id}")
        pass
    
    def get_user_stats(self, user_id: str) -> Dict:
        """
        Возвращает статистику пользователя
        
        Args:
            user_id: ID пользователя
        
        Returns:
            Dict: Статистика пользователя
        """
        # Возвращаем базовую статистику
        return {
            'total_searches': 0,
            'total_offers_found': 0,
            'last_search': None,
            'registered_at': datetime.now().isoformat()
        }
    
    def get_database_stats(self) -> Dict:
        """
        Возвращает общую статистику базы данных
        
        Returns:
            Dict: Статистика БД
        """
        stats = self.get_statistics()
        return {
            'total_offers': stats.get('total_listings', 0),
            'total_users': 1,  # Упрощенно
            'database_size_mb': 0.1,  # Примерно
            'last_updated': datetime.now().isoformat()
        }

# Создаем глобальный экземпляр менеджера
databd_manager = DataBDManager()

def main():
    """Тестирование менеджера базы данных"""
    print("🗄️ ТЕСТ МЕНЕДЖЕРА БАЗЫ ДАННЫХ dataBD")
    print("=" * 50)
    
    try:
        # Начинаем сессию парсинга
        session_id = databd_manager.start_parsing_session("cian", "Тестовая сессия")
        print(f"✅ Создана сессия парсинга: {session_id}")
        
        # Тестовое объявление
        test_listing = {
            'id': 'test_123456',
            'source': 'cian',
            'price': 150000,
            'area': '100 м²',
            'description': 'Тестовое коммерческое помещение в центре города',
            'url': 'https://cian.ru/test/123456',
            'floor': '1',
            'address': 'Тестовая улица, 1',
            'lat': '55.751244',
            'lng': '37.618423',
            'seller': 'Тестовый риелтор | https://cian.ru/agent/test',
            'photos': ['photo1.jpg', 'photo2.jpg'],
            'status': 'open',
            'visible': 1
        }
        
        # Сохраняем объявление
        is_new = databd_manager.save_listing(test_listing)
        print(f"✅ Сохранено объявление (новое: {is_new})")
        
        # Получаем объявление обратно
        retrieved = databd_manager.get_listing_by_id('test_123456')
        if retrieved:
            print(f"✅ Получено объявление: {retrieved['address']}")
        
        # Получаем список объявлений
        listings = databd_manager.get_listings(limit=5)
        print(f"✅ Получено {len(listings)} объявлений")
        
        # Получаем статистику
        stats = databd_manager.get_statistics()
        print(f"✅ Статистика:")
        print(f"   Всего объявлений: {stats.get('total_listings', 0)}")
        print(f"   Видимых: {stats.get('visible_listings', 0)}")
        print(f"   Средняя цена: {stats.get('avg_price', 0):.0f} ₽")
        
        # Завершаем сессию
        databd_manager.finish_parsing_session(session_id, 1, 1)
        print(f"✅ Завершена сессия парсинга")
        
        # Обновляем ежедневную статистику
        databd_manager.update_daily_stats()
        print(f"✅ Обновлена ежедневная статистика")
        
        print(f"\n🎉 Тест завершен успешно!")
        print(f"📁 База данных создана: {databd_manager.db_path}")
        
    except Exception as e:
        print(f"❌ Ошибка теста: {e}")

if __name__ == "__main__":
    main() 
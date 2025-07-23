#!/usr/bin/env python3
"""
Модуль для просмотра и анализа базы данных объявлений
Предоставляет удобные функции для работы с cian_reports.db
"""

import sqlite3
import os
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class DatabaseViewer:
    """Просмотрщик базы данных объявлений"""
    
    def __init__(self, db_path: str = "dataBD/cian_reports.db"):
        self.db_path = db_path
    
    def get_all_listings(self, limit: int = 50) -> List[Dict]:
        """Получает все объявления из БД"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row  # Для доступа по имени колонки
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT * FROM cian_reports 
                    ORDER BY created_at DESC 
                    LIMIT ?
                """, (limit,))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка получения объявлений: {e}")
            return []
    
    def get_price_statistics(self) -> Dict:
        """Получает статистику по ценам"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_count,
                        AVG(price) as avg_price,
                        MIN(price) as min_price,
                        MAX(price) as max_price,
                        COUNT(CASE WHEN price < 50000 THEN 1 END) as under_50k,
                        COUNT(CASE WHEN price BETWEEN 50000 AND 100000 THEN 1 END) as 50k_100k,
                        COUNT(CASE WHEN price > 100000 THEN 1 END) as over_100k
                    FROM cian_reports 
                    WHERE price > 0
                """)
                
                row = cursor.fetchone()
                if row:
                    return {
                        'total_count': row[0],
                        'avg_price': round(row[1], 2) if row[1] else 0,
                        'min_price': row[2] or 0,
                        'max_price': row[3] or 0,
                        'under_50k': row[4],
                        '50k_100k': row[5], 
                        'over_100k': row[6]
                    }
                    
        except Exception as e:
            logger.error(f"Ошибка получения статистики цен: {e}")
            
        return {}
    
    def search_by_price_range(self, min_price: int, max_price: int) -> List[Dict]:
        """Поиск объявлений по диапазону цен"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT * FROM cian_reports 
                    WHERE price BETWEEN ? AND ?
                    ORDER BY price ASC
                """, (min_price, max_price))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка поиска по ценам: {e}")
            return []
    
    def search_by_address(self, address_part: str) -> List[Dict]:
        """Поиск объявлений по части адреса"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT * FROM cian_reports 
                    WHERE address LIKE ?
                    ORDER BY price ASC
                """, (f'%{address_part}%',))
                
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"Ошибка поиска по адресу: {e}")
            return []
    
    def get_formatted_listing(self, listing: Dict) -> str:
        """Форматирует объявление для красивого вывода"""
        try:
            price_text = f"{listing['price']:,.0f} ₽/мес".replace(',', ' ')
            
            formatted = f"""
🏢 Объявление #{listing['id']}
💰 Цена: {price_text}
📏 Площадь: {listing['area']}
📍 Адрес: {listing['address']}
🏗️ Этаж: {listing['floor']}
📞 Контакт: {listing['seller']}
🔗 Ссылка: {listing['url']}
📝 Описание: {listing['description']}
📅 Статус: {listing['status']}
"""
            return formatted.strip()
            
        except Exception as e:
            logger.error(f"Ошибка форматирования объявления: {e}")
            return f"Ошибка отображения объявления {listing.get('id', 'неизвестно')}"
    
    def export_to_txt(self, filename: str = "dataBD/cian_report_export.txt") -> bool:
        """Экспортирует все объявления в текстовый файл"""
        try:
            listings = self.get_all_listings(limit=1000)
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("           ОТЧЕТ ПО ОБЪЯВЛЕНИЯМ НЕДВИЖИМОСТИ\n")
                f.write("=" * 80 + "\n\n")
                
                stats = self.get_price_statistics()
                f.write(f"📊 Общая статистика:\n")
                f.write(f"   Всего объявлений: {stats.get('total_count', 0)}\n")
                f.write(f"   Средняя цена: {stats.get('avg_price', 0):,.0f} ₽/мес\n")
                f.write(f"   Диапазон цен: {stats.get('min_price', 0):,.0f} - {stats.get('max_price', 0):,.0f} ₽/мес\n")
                f.write(f"   До 50,000 ₽: {stats.get('under_50k', 0)} объявлений\n")
                f.write(f"   50,000-100,000 ₽: {stats.get('50k_100k', 0)} объявлений\n")
                f.write(f"   Свыше 100,000 ₽: {stats.get('over_100k', 0)} объявлений\n\n")
                
                f.write("=" * 80 + "\n")
                f.write("                    СПИСОК ОБЪЯВЛЕНИЙ\n")
                f.write("=" * 80 + "\n\n")
                
                for i, listing in enumerate(listings, 1):
                    f.write(f"[{i}] {self.get_formatted_listing(listing)}\n")
                    f.write("-" * 50 + "\n\n")
            
            logger.info(f"✅ Экспорт завершен: {filename}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка экспорта: {e}")
            return False
    
    def generate_summary_report(self) -> str:
        """Генерирует краткий отчет по БД"""
        try:
            stats = self.get_price_statistics()
            listings = self.get_all_listings(limit=5)
            
            report = f"""
📊 КРАТКИЙ ОТЧЕТ ПО БАЗЕ ДАННЫХ

🗄️ База данных: {os.path.basename(self.db_path)}
📁 Размер файла: {os.path.getsize(self.db_path) / 1024:.1f} KB

📈 Статистика:
   • Всего объявлений: {stats.get('total_count', 0)}
   • Средняя цена: {stats.get('avg_price', 0):,.0f} ₽/мес
   • Самое дешевое: {stats.get('min_price', 0):,.0f} ₽/мес
   • Самое дорогое: {stats.get('max_price', 0):,.0f} ₽/мес

💰 Распределение по ценам:
   • До 50,000 ₽: {stats.get('under_50k', 0)} ({stats.get('under_50k', 0) / max(stats.get('total_count', 1), 1) * 100:.1f}%)
   • 50,000-100,000 ₽: {stats.get('50k_100k', 0)} ({stats.get('50k_100k', 0) / max(stats.get('total_count', 1), 1) * 100:.1f}%)
   • Свыше 100,000 ₽: {stats.get('over_100k', 0)} ({stats.get('over_100k', 0) / max(stats.get('total_count', 1), 1) * 100:.1f}%)

🏢 Примеры объявлений:
"""
            
            for listing in listings[:3]:
                price = f"{listing['price']:,.0f} ₽".replace(',', ' ')
                report += f"   • {price} - {listing['area']} - {listing['address']}\n"
            
            return report.strip()
            
        except Exception as e:
            logger.error(f"Ошибка генерации отчета: {e}")
            return "❌ Ошибка генерации отчета"

# Функции для быстрого использования
def view_all_listings(limit: int = 10):
    """Быстрый просмотр объявлений"""
    viewer = DatabaseViewer()
    listings = viewer.get_all_listings(limit)
    
    print(f"📋 Найдено {len(listings)} объявлений:")
    print("=" * 80)
    
    for i, listing in enumerate(listings, 1):
        print(f"[{i}] {viewer.get_formatted_listing(listing)}")
        print("-" * 50)

def search_by_price(min_price: int, max_price: int):
    """Быстрый поиск по цене"""
    viewer = DatabaseViewer()
    listings = viewer.search_by_price_range(min_price, max_price)
    
    print(f"💰 Найдено {len(listings)} объявлений в диапазоне {min_price:,} - {max_price:,} ₽:")
    print("=" * 80)
    
    for listing in listings:
        print(viewer.get_formatted_listing(listing))
        print("-" * 50)

def show_stats():
    """Показать статистику БД"""
    viewer = DatabaseViewer()
    print(viewer.generate_summary_report())

# Создаем глобальный экземпляр
db_viewer = DatabaseViewer() 
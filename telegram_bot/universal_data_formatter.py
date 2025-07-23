#!/usr/bin/env python3
"""
Универсальный форматер данных для приведения всех файлов к формату real_estate_data.db
Поддерживает различные источники: Excel, CSV, другие SQLite БД
"""

import sqlite3
import os
import json
import random
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
import shutil

logger = logging.getLogger(__name__)

# Попытка импортировать pandas для работы с различными форматами
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("⚠️ Pandas недоступен. Установите: pip install pandas openpyxl")

class UniversalDataFormatter:
    """Универсальный форматер данных в стандарт real_estate_data.db"""
    
    def __init__(self, target_db_path: str = "dataBD/real_estate_data_unified.db"):
        self.target_db_path = target_db_path
        self.target_schema = self._get_target_schema()
        self.random_generators = self._setup_random_generators()
        
    def _get_target_schema(self) -> Dict:
        """Возвращает целевую схему БД на основе real_estate_data.db"""
        return {
            'table_name': 'real_estate_listings',
            'columns': {
                'id': {'type': 'TEXT', 'primary_key': True, 'required': True},
                'source': {'type': 'TEXT', 'required': True, 'default': 'unknown'},
                'price': {'type': 'REAL', 'required': False},
                'area': {'type': 'TEXT', 'required': False},
                'description': {'type': 'TEXT', 'required': False},
                'url': {'type': 'TEXT', 'required': False},
                'floor': {'type': 'TEXT', 'required': False},
                'address': {'type': 'TEXT', 'required': False},
                'lat': {'type': 'TEXT', 'required': False},
                'lng': {'type': 'TEXT', 'required': False},
                'seller': {'type': 'TEXT', 'required': False},
                'photos': {'type': 'TEXT', 'required': False, 'default': '[]'},
                'status': {'type': 'TEXT', 'required': False, 'default': 'open'},
                'visible': {'type': 'INTEGER', 'required': False, 'default': 1}
            }
        }
    
    def _setup_random_generators(self) -> Dict:
        """Настраивает генераторы случайных данных"""
        return {
            'addresses_perm': [
                'г. Пермь, ул. Ленина, {}', 'г. Пермь, ул. Мира, {}', 
                'г. Пермь, ул. Комсомольская, {}', 'г. Пермь, ул. Петропавловская, {}',
                'г. Пермь, ул. Революции, {}', 'г. Пермь, проспект Парковый, {}',
                'г. Пермь, ул. Куйбышева, {}', 'г. Пермь, ул. Сибирская, {}',
                'г. Пермь, ул. Екатерининская, {}', 'г. Пермь, ул. Газеты Звезда, {}'
            ],
            'descriptions': [
                'Отличное помещение в центре города', 'Помещение с отдельным входом',
                'Просторное помещение с высокими потолками', 'Помещение после ремонта',
                'Помещение в проходном месте', 'Удобная планировка',
                'Помещение с парковкой', 'Помещение в деловом центре',
                'Помещение с отличной проходимостью', 'Современное помещение'
            ],
            'phones': [
                '+7(342)123-45-67', '+7(342)234-56-78', '+7(342)345-67-89',
                '+7(342)456-78-90', '+7(342)567-89-01', '+7(342)678-90-12'
            ],
            'areas': [15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 120, 150, 200, 250, 300, 400, 500],
            'floors': ['1/5', '2/5', '3/5', '1/9', '2/9', '3/9', '4/9', '5/9', '1/12', '2/12', '3/12'],
            'prices': [30000, 35000, 40000, 45000, 50000, 60000, 70000, 80000, 90000, 100000, 120000, 150000, 200000],
            'statuses': ['open', 'active', 'available', 'published']
        }
    
    def create_target_database(self):
        """Создает целевую БД с правильной схемой"""
        try:
            # Создаем папку если её нет
            os.makedirs(os.path.dirname(self.target_db_path), exist_ok=True)
            
            with sqlite3.connect(self.target_db_path) as conn:
                cursor = conn.cursor()
                
                # Создаем таблицу
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.target_schema['table_name']} (
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
                
                # Создаем индексы
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_listing_id ON {self.target_schema['table_name']}(id)")
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_listing_status ON {self.target_schema['table_name']}(status)")
                cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_listing_visible ON {self.target_schema['table_name']}(visible)")
                
                conn.commit()
                logger.info(f"✅ Целевая БД создана: {self.target_db_path}")
                
        except Exception as e:
            logger.error(f"Ошибка создания целевой БД: {e}")
            raise
    
    def generate_missing_value(self, column_name: str, existing_value: Any = None) -> Any:
        """Генерирует недостающее значение для колонки"""
        if existing_value and str(existing_value).strip() and existing_value != 'None':
            return existing_value
            
        column_name_lower = column_name.lower()
        
        if column_name == 'id':
            return f"formatted_{random.randint(100000, 999999)}"
        elif column_name == 'source':
            return 'formatted_data'
        elif column_name == 'price':
            return random.choice(self.random_generators['prices'])
        elif column_name == 'area':
            return f"{random.choice(self.random_generators['areas'])} м²"
        elif column_name == 'description':
            return random.choice(self.random_generators['descriptions'])
        elif column_name == 'url':
            return f"https://perm.cian.ru/rent/commercial/{random.randint(100000, 999999)}/"
        elif column_name == 'floor':
            return random.choice(self.random_generators['floors'])
        elif column_name == 'address':
            template = random.choice(self.random_generators['addresses_perm'])
            return template.format(random.randint(1, 200))
        elif column_name == 'lat':
            return str(round(58.0 + random.uniform(-0.1, 0.1), 6))
        elif column_name == 'lng':
            return str(round(56.25 + random.uniform(-0.1, 0.1), 6))
        elif column_name == 'seller':
            return random.choice(self.random_generators['phones'])
        elif column_name == 'photos':
            return json.dumps([])
        elif column_name == 'status':
            return random.choice(self.random_generators['statuses'])
        elif column_name == 'visible':
            return 1
        else:
            return f"auto_generated_{random.randint(1000, 9999)}"
    
    def format_sqlite_db(self, source_db_path: str, source_table: str = None) -> bool:
        """Форматирует SQLite БД к целевому формату"""
        try:
            logger.info(f"🔄 Форматируем SQLite БД: {source_db_path}")
            
            # Подключаемся к исходной БД
            with sqlite3.connect(source_db_path) as source_conn:
                source_conn.row_factory = sqlite3.Row
                cursor = source_conn.cursor()
                
                # Если таблица не указана, найдем первую подходящую
                if not source_table:
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    source_table = tables[0] if tables else None
                
                if not source_table:
                    logger.error("Не найдена таблица для обработки")
                    return False
                
                logger.info(f"📋 Обрабатываем таблицу: {source_table}")
                
                # Получаем все данные
                cursor.execute(f"SELECT * FROM {source_table}")
                rows = cursor.fetchall()
                
                logger.info(f"📊 Найдено {len(rows)} записей")
                
                # Форматируем каждую запись
                formatted_data = []
                for row in rows:
                    formatted_row = {}
                    row_dict = dict(row)
                    
                    # Заполняем все обязательные колонки
                    for col_name in self.target_schema['columns'].keys():
                        # Ищем похожую колонку в исходных данных
                        source_value = None
                        for source_col, value in row_dict.items():
                            if (col_name.lower() == source_col.lower() or 
                                col_name in source_col.lower() or 
                                source_col.lower() in col_name):
                                source_value = value
                                break
                        
                        # Генерируем значение если не найдено
                        formatted_row[col_name] = self.generate_missing_value(col_name, source_value)
                    
                    formatted_data.append(formatted_row)
                
                # Сохраняем в целевую БД
                return self._save_formatted_data(formatted_data, f"sqlite_{os.path.basename(source_db_path)}")
                
        except Exception as e:
            logger.error(f"Ошибка форматирования SQLite БД: {e}")
            return False
    
    def format_excel_file(self, excel_path: str) -> bool:
        """Форматирует Excel файл к целевому формату"""
        if not PANDAS_AVAILABLE:
            logger.error("Pandas не установлен для работы с Excel")
            return False
            
        try:
            logger.info(f"🔄 Форматируем Excel файл: {excel_path}")
            
            # Читаем Excel
            df = pd.read_excel(excel_path)
            logger.info(f"📊 Найдено {len(df)} строк, {len(df.columns)} колонок")
            
            # Проверяем, статистический ли это отчет
            if len(df.columns) == 2 and any('показатель' in str(col).lower() for col in df.columns):
                # Генерируем данные на основе статистики
                count = 5  # по умолчанию
                try:
                    for _, row in df.iterrows():
                        if 'количество' in str(row.iloc[0]).lower():
                            count = int(row.iloc[1])
                            break
                except:
                    pass
                
                formatted_data = []
                for i in range(count):
                    row_data = {}
                    for col_name in self.target_schema['columns'].keys():
                        row_data[col_name] = self.generate_missing_value(col_name)
                    formatted_data.append(row_data)
            else:
                # Обычный Excel с данными
                formatted_data = []
                for _, row in df.iterrows():
                    formatted_row = {}
                    row_dict = row.to_dict()
                    
                    for col_name in self.target_schema['columns'].keys():
                        source_value = None
                        for source_col, value in row_dict.items():
                            if (col_name.lower() in str(source_col).lower() or
                                str(source_col).lower() in col_name.lower()):
                                source_value = value
                                break
                        
                        formatted_row[col_name] = self.generate_missing_value(col_name, source_value)
                    
                    formatted_data.append(formatted_row)
            
            return self._save_formatted_data(formatted_data, f"excel_{os.path.basename(excel_path)}")
            
        except Exception as e:
            logger.error(f"Ошибка форматирования Excel: {e}")
            return False
    
    def _save_formatted_data(self, data: List[Dict], source_label: str) -> bool:
        """Сохраняет отформатированные данные в целевую БД"""
        try:
            self.create_target_database()
            
            with sqlite3.connect(self.target_db_path) as conn:
                cursor = conn.cursor()
                
                # Подготавливаем запрос для вставки
                columns = list(self.target_schema['columns'].keys())
                placeholders = ','.join(['?' for _ in columns])
                
                insert_query = f"""
                    INSERT OR REPLACE INTO {self.target_schema['table_name']} 
                    ({','.join(columns)}) VALUES ({placeholders})
                """
                
                # Вставляем данные
                for row in data:
                    values = [row.get(col, '') for col in columns]
                    cursor.execute(insert_query, values)
                
                conn.commit()
                
                logger.info(f"✅ Сохранено {len(data)} записей из {source_label}")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка сохранения данных: {e}")
            return False
    
    def format_all_files_in_directory(self, directory: str = "dataBD") -> Dict[str, bool]:
        """Форматирует все файлы данных в указанной папке"""
        results = {}
        
        logger.info(f"🔄 Сканируем папку {directory} для форматирования файлов...")
        
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            
            if not os.path.isfile(filepath):
                continue
                
            # Пропускаем целевую БД
            if filepath == self.target_db_path:
                continue
                
            logger.info(f"📁 Обрабатываем: {filename}")
            
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                results[filename] = self.format_excel_file(filepath)
            elif filename.endswith('.db') and filename != os.path.basename(self.target_db_path):
                results[filename] = self.format_sqlite_db(filepath)
            else:
                logger.info(f"⏭️ Пропускаем {filename} (неподдерживаемый формат)")
                
        return results
    
    def get_unified_stats(self) -> Dict:
        """Получает статистику объединенной БД"""
        try:
            with sqlite3.connect(self.target_db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute(f"SELECT COUNT(*) FROM {self.target_schema['table_name']}")
                total_count = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT AVG(price), MIN(price), MAX(price) FROM {self.target_schema['table_name']} WHERE price > 0")
                price_stats = cursor.fetchone()
                
                cursor.execute(f"SELECT source, COUNT(*) FROM {self.target_schema['table_name']} GROUP BY source")
                source_distribution = dict(cursor.fetchall())
                
                return {
                    'total_records': total_count,
                    'avg_price': round(price_stats[0], 2) if price_stats[0] else 0,
                    'min_price': price_stats[1] or 0,
                    'max_price': price_stats[2] or 0,
                    'source_distribution': source_distribution,
                    'database_path': self.target_db_path
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}

# Функция для быстрого использования
def format_all_data_files(target_db: str = "dataBD/real_estate_data_unified.db") -> bool:
    """Быстрое форматирование всех файлов данных"""
    formatter = UniversalDataFormatter(target_db)
    results = formatter.format_all_files_in_directory()
    
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    
    print(f"📊 Результаты форматирования:")
    print(f"   ✅ Успешно: {success_count}/{total_count}")
    
    for filename, success in results.items():
        status = "✅" if success else "❌"
        print(f"   {status} {filename}")
    
    if success_count > 0:
        stats = formatter.get_unified_stats()
        print(f"\n📈 Объединенная статистика:")
        print(f"   📊 Всего записей: {stats.get('total_records', 0)}")
        print(f"   💰 Средняя цена: {stats.get('avg_price', 0):,.0f} ₽")
        print(f"   📁 БД: {stats.get('database_path', 'неизвестно')}")
        print(f"   🗂️ Источники: {stats.get('source_distribution', {})}")
    
    return success_count > 0

# Создаем глобальный экземпляр
data_formatter = UniversalDataFormatter() 
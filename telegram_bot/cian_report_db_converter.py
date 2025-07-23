#!/usr/bin/env python3
"""
Модуль для конвертации Excel отчетов Cian в формат базы данных
Автоматически заполняет недостающие данные реалистичными случайными значениями
"""

import sqlite3
import os
import random
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json

logger = logging.getLogger(__name__)

# Попытка импортировать pandas для работы с Excel
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
    logger.info("✅ Pandas доступен для работы с Excel")
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("⚠️ Pandas недоступен. Установите: pip install pandas openpyxl")

class CianReportDBConverter:
    """Конвертер Excel отчетов в базу данных"""
    
    def __init__(self, excel_file_path: str = "dataBD/cian_report.xlsx"):
        self.excel_file_path = excel_file_path
        self.output_db_path = "dataBD/cian_reports.db"
        self.random_data_generators = self._setup_random_generators()
    
    def _setup_random_generators(self) -> Dict:
        """Настраивает генераторы случайных данных"""
        return {
            'addresses': [
                'г. Пермь, ул. Ленина, {}',
                'г. Пермь, ул. Мира, {}', 
                'г. Пермь, ул. Комсомольская, {}',
                'г. Пермь, ул. Петропавловская, {}',
                'г. Пермь, ул. Революции, {}',
                'г. Пермь, проспект Парковый, {}',
                'г. Пермь, ул. Куйбышева, {}',
                'г. Пермь, ул. Сибирская, {}',
                'г. Пермь, ул. Екатерининская, {}',
                'г. Пермь, ул. Газеты Звезда, {}'
            ],
            'property_types': [
                'Офис', 'Торговое помещение', 'Складское помещение', 
                'Производственное помещение', 'Свободное назначение',
                'Общепит', 'Автосервис', 'Медицинский центр',
                'Салон красоты', 'Автомойка'
            ],
            'descriptions': [
                'Отличное помещение в центре города',
                'Помещение с отдельным входом',
                'Просторное помещение с высокими потолками',
                'Помещение после ремонта',
                'Помещение в проходном месте',
                'Удобная планировка',
                'Помещение с парковкой',
                'Помещение в деловом центре',
                'Помещение с отличной проходимостью',
                'Современное помещение'
            ],
            'contact_phones': [
                '+7(342)123-45-67', '+7(342)234-56-78', '+7(342)345-67-89',
                '+7(342)456-78-90', '+7(342)567-89-01', '+7(342)678-90-12',
                '+7(342)789-01-23', '+7(342)890-12-34', '+7(342)901-23-45',
                '+7(342)012-34-56'
            ],
            'areas': [15, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100, 120, 150, 200, 250, 300, 400, 500],
            'floors': ['1/5', '2/5', '3/5', '1/9', '2/9', '3/9', '4/9', '5/9', '1/12', '2/12', '3/12'],
            'prices': [30000, 35000, 40000, 45000, 50000, 60000, 70000, 80000, 90000, 100000, 120000, 150000, 200000]
        }
    
    def read_excel_report(self) -> Optional[pd.DataFrame]:
        """Читает Excel файл отчета"""
        if not PANDAS_AVAILABLE:
            logger.error("Pandas не установлен. Используйте: pip install pandas openpyxl")
            return None
            
        try:
            if not os.path.exists(self.excel_file_path):
                logger.error(f"Файл {self.excel_file_path} не найден")
                return None
            
            # Пробуем разные способы чтения Excel
            try:
                df = pd.read_excel(self.excel_file_path)
                logger.info(f"✅ Excel файл загружен: {len(df)} строк")
                
                # Проверяем формат файла
                if len(df.columns) == 2 and any('показатель' in str(col).lower() for col in df.columns):
                    logger.info("🔄 Обнаружен статистический отчет, генерируем объявления...")
                    return self._generate_listings_from_stats(df)
                
                return df
            except Exception as e:
                # Пробуем с указанием engine
                df = pd.read_excel(self.excel_file_path, engine='openpyxl')
                logger.info(f"✅ Excel файл загружен (openpyxl): {len(df)} строк")
                return df
                
        except Exception as e:
            logger.error(f"Ошибка чтения Excel файла: {e}")
            return None
    
    def _generate_listings_from_stats(self, stats_df: pd.DataFrame) -> pd.DataFrame:
        """Генерирует объявления на основе статистики из отчета"""
        logger.info("🏗️ Генерируем объявления на основе статистики...")
        
        # Извлекаем количество объявлений из статистики
        listings_count = 5  # значение по умолчанию
        
        try:
            for _, row in stats_df.iterrows():
                if 'количество' in str(row.iloc[0]).lower() and 'объявлений' in str(row.iloc[0]).lower():
                    try:
                        listings_count = int(row.iloc[1])
                        logger.info(f"📊 Найдено в статистике: {listings_count} объявлений")
                        break
                    except:
                        pass
        except Exception as e:
            logger.warning(f"Не удалось извлечь количество из статистики: {e}")
        
        # Генерируем указанное количество объявлений
        generated_listings = []
        
        for i in range(max(listings_count, 1)):  # минимум 1 объявление
            listing = {
                'id': f"generated_{random.randint(100000, 999999)}",
                'source': 'cian',
                'price': random.choice(self.random_data_generators['prices']),
                'area': f"{random.choice(self.random_data_generators['areas'])} м²",
                'description': random.choice(self.random_data_generators['descriptions']),
                'url': f"https://perm.cian.ru/rent/commercial/{random.randint(100000, 999999)}/",
                'floor': random.choice(self.random_data_generators['floors']),
                'address': random.choice(self.random_data_generators['addresses']).format(random.randint(1, 200)),
                'lat': round(58.0 + random.uniform(-0.1, 0.1), 6),
                'lng': round(56.25 + random.uniform(-0.1, 0.1), 6),
                'seller': random.choice(self.random_data_generators['contact_phones']),
                'photos': json.dumps([]),
                'status': random.choice(['open', 'active', 'available']),
                'visible': 1
            }
            generated_listings.append(listing)
        
        # Создаем DataFrame из сгенерированных объявлений
        df = pd.DataFrame(generated_listings)
        logger.info(f"✅ Сгенерировано {len(df)} объявлений")
        
        return df
    
    def generate_random_value(self, field_name: str, existing_value: Any = None) -> Any:
        """Генерирует случайное значение для поля"""
        if existing_value and pd.notna(existing_value) and str(existing_value).strip():
            return existing_value
        
        field_name_lower = field_name.lower()
        
        if 'price' in field_name_lower or 'цена' in field_name_lower:
            return random.choice(self.random_data_generators['prices'])
        
        elif 'area' in field_name_lower or 'площадь' in field_name_lower:
            return random.choice(self.random_data_generators['areas'])
        
        elif 'address' in field_name_lower or 'адрес' in field_name_lower:
            template = random.choice(self.random_data_generators['addresses'])
            return template.format(random.randint(1, 200))
        
        elif 'type' in field_name_lower or 'тип' in field_name_lower:
            return random.choice(self.random_data_generators['property_types'])
        
        elif 'floor' in field_name_lower or 'этаж' in field_name_lower:
            return random.choice(self.random_data_generators['floors'])
        
        elif 'phone' in field_name_lower or 'телефон' in field_name_lower:
            return random.choice(self.random_data_generators['contact_phones'])
        
        elif 'description' in field_name_lower or 'описание' in field_name_lower:
            return random.choice(self.random_data_generators['descriptions'])
        
        elif 'url' in field_name_lower or 'ссылка' in field_name_lower:
            return f"https://perm.cian.ru/rent/commercial/{random.randint(100000, 999999)}/"
        
        elif 'id' in field_name_lower:
            return random.randint(100000, 999999)
        
        elif 'source' in field_name_lower or 'источник' in field_name_lower:
            return 'cian'
        
        elif 'status' in field_name_lower or 'статус' in field_name_lower:
            return random.choice(['open', 'active', 'available'])
        
        elif 'visible' in field_name_lower:
            return 1
        
        elif 'lat' in field_name_lower or 'latitude' in field_name_lower:
            # Координаты для Перми
            return round(58.0 + random.uniform(-0.1, 0.1), 6)
        
        elif 'lng' in field_name_lower or 'longitude' in field_name_lower:
            # Координаты для Перми  
            return round(56.25 + random.uniform(-0.1, 0.1), 6)
        
        elif 'date' in field_name_lower or 'время' in field_name_lower:
            # Случайная дата в последние 30 дней
            days_ago = random.randint(0, 30)
            return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d %H:%M:%S')
        
        else:
            # Общие значения по умолчанию
            return f"Автозаполнено_{random.randint(1000, 9999)}"
    
    def normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормализует DataFrame к стандартному формату БД"""
        # Стандартные колонки для БД объявлений
        standard_columns = {
            'id': 'id',
            'source': 'source', 
            'price': 'price',
            'area': 'area',
            'description': 'description',
            'url': 'url',
            'floor': 'floor',
            'address': 'address',
            'lat': 'lat',
            'lng': 'lng',
            'seller': 'seller',
            'photos': 'photos',
            'status': 'status',
            'visible': 'visible'
        }
        
        # Создаем новый DataFrame с стандартными колонками
        normalized_df = pd.DataFrame()
        
        # Пытаемся мапить существующие колонки
        for std_col, _ in standard_columns.items():
            found_column = None
            
            # Ищем похожие колонки в исходных данных
            for col in df.columns:
                col_lower = str(col).lower()
                if (std_col.lower() in col_lower or 
                    (std_col == 'price' and ('цена' in col_lower or 'стоимость' in col_lower)) or
                    (std_col == 'area' and ('площадь' in col_lower or 'кв' in col_lower)) or
                    (std_col == 'address' and ('адрес' in col_lower or 'расположение' in col_lower)) or
                    (std_col == 'description' and ('описание' in col_lower or 'комментарий' in col_lower))):
                    found_column = col
                    break
            
            if found_column:
                normalized_df[std_col] = df[found_column]
                logger.info(f"✅ Мапинг: {found_column} -> {std_col}")
            else:
                # Создаем колонку с пустыми значениями для заполнения
                normalized_df[std_col] = None
                logger.info(f"⚠️ Колонка {std_col} будет заполнена автоматически")
        
        return normalized_df
    
    def fill_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Заполняет недостающие данные случайными значениями"""
        logger.info("🎲 Заполняем недостающие данные...")
        
        filled_df = df.copy()
        
        for column in filled_df.columns:
            missing_count = filled_df[column].isna().sum()
            if missing_count > 0:
                logger.info(f"Заполняем {missing_count} пустых значений в колонке '{column}'")
                
                for idx in filled_df[filled_df[column].isna()].index:
                    filled_df.at[idx, column] = self.generate_random_value(column)
        
        # Специальная обработка для photos (JSON массив)
        if 'photos' in filled_df.columns:
            filled_df['photos'] = filled_df['photos'].apply(
                lambda x: json.dumps([]) if pd.isna(x) else x
            )
        
        logger.info("✅ Все недостающие данные заполнены")
        return filled_df
    
    def create_database_schema(self):
        """Создает схему базы данных для отчетов"""
        try:
            # Создаем папку если её нет
            os.makedirs(os.path.dirname(self.output_db_path), exist_ok=True)
            
            with sqlite3.connect(self.output_db_path) as conn:
                cursor = conn.cursor()
                
                # Создаем таблицу для отчетов
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cian_reports (
                        id TEXT PRIMARY KEY,
                        source TEXT NOT NULL DEFAULT 'cian',
                        price REAL,
                        area TEXT,
                        description TEXT,
                        url TEXT,
                        floor TEXT,
                        address TEXT,
                        lat TEXT,
                        lng TEXT,
                        seller TEXT,
                        photos TEXT DEFAULT '[]',
                        status TEXT DEFAULT 'open',
                        visible INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Создаем индексы
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_reports_price ON cian_reports(price)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_reports_area ON cian_reports(area)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_reports_status ON cian_reports(status)")
                
                conn.commit()
                logger.info(f"✅ База данных создана: {self.output_db_path}")
                
        except Exception as e:
            logger.error(f"Ошибка создания БД: {e}")
            raise
    
    def save_to_database(self, df: pd.DataFrame) -> bool:
        """Сохраняет DataFrame в базу данных"""
        try:
            self.create_database_schema()
            
            with sqlite3.connect(self.output_db_path) as conn:
                # Очищаем таблицу перед вставкой новых данных
                conn.execute("DELETE FROM cian_reports")
                
                # Сохраняем данные
                df.to_sql('cian_reports', conn, if_exists='append', index=False)
                
                # Подсчитываем количество записей
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM cian_reports")
                count = cursor.fetchone()[0]
                
                logger.info(f"✅ Сохранено {count} записей в базу данных")
                return True
                
        except Exception as e:
            logger.error(f"Ошибка сохранения в БД: {e}")
            return False
    
    def convert_excel_to_db(self) -> bool:
        """Главная функция конвертации Excel в БД"""
        logger.info("🔄 Начинаем конвертацию Excel отчета в базу данных...")
        
        # Шаг 1: Читаем Excel файл
        df = self.read_excel_report()
        if df is None:
            return False
        
        logger.info(f"📊 Исходные данные: {len(df)} строк, {len(df.columns)} колонок")
        logger.info(f"📋 Колонки: {list(df.columns)}")
        
        # Шаг 2: Нормализуем структуру
        normalized_df = self.normalize_dataframe(df)
        
        # Шаг 3: Заполняем недостающие данные
        filled_df = self.fill_missing_data(normalized_df)
        
        # Шаг 4: Сохраняем в БД
        success = self.save_to_database(filled_df)
        
        if success:
            logger.info(f"🎉 Конвертация завершена успешно!")
            logger.info(f"📁 База данных: {self.output_db_path}")
            return True
        else:
            logger.error("❌ Ошибка конвертации")
            return False
    
    def get_database_stats(self) -> Dict:
        """Возвращает статистику по созданной БД"""
        try:
            with sqlite3.connect(self.output_db_path) as conn:
                cursor = conn.cursor()
                
                # Общее количество записей
                cursor.execute("SELECT COUNT(*) FROM cian_reports")
                total_count = cursor.fetchone()[0]
                
                # Средняя цена
                cursor.execute("SELECT AVG(price) FROM cian_reports WHERE price > 0")
                avg_price = cursor.fetchone()[0] or 0
                
                # Диапазон цен
                cursor.execute("SELECT MIN(price), MAX(price) FROM cian_reports WHERE price > 0")
                min_price, max_price = cursor.fetchone()
                
                # Количество по типам статуса
                cursor.execute("SELECT status, COUNT(*) FROM cian_reports GROUP BY status")
                status_counts = dict(cursor.fetchall())
                
                return {
                    'total_records': total_count,
                    'avg_price': round(avg_price, 2) if avg_price else 0,
                    'min_price': min_price or 0,
                    'max_price': max_price or 0,
                    'status_distribution': status_counts,
                    'database_file': self.output_db_path
                }
                
        except Exception as e:
            logger.error(f"Ошибка получения статистики: {e}")
            return {}

# Функция для быстрого использования
def convert_cian_excel_to_db(excel_path: str = "dataBD/cian_report.xlsx") -> bool:
    """Быстрая конвертация Excel отчета в БД"""
    converter = CianReportDBConverter(excel_path)
    return converter.convert_excel_to_db()

# Создаем глобальный экземпляр
db_converter = CianReportDBConverter() 
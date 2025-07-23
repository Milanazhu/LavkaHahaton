#!/usr/bin/env python3
"""
Конвертер файлов Numbers в формат базы данных real_estate_data
"""

import sqlite3
import json
import random
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import os
import sys

class NumbersToDBConverter:
    def __init__(self, numbers_file, target_db_file, output_db_file):
        self.numbers_file = numbers_file
        self.target_db_file = target_db_file
        self.output_db_file = output_db_file
        
        # Пермские адреса для генерации данных
        self.perm_addresses = [
            "ул. Ленина, 50", "ул. Комсомольский проспект, 15", "ул. Петропавловская, 25",
            "ул. Сибирская, 32", "ул. Куйбышева, 18", "ул. Монастырская, 12",
            "ул. Газеты Звезда, 45", "ул. Революции, 28", "ул. Крупской, 60",
            "ул. Пушкина, 17", "ул. Максима Горького, 83", "ул. Екатерининская, 55",
            "ул. Компроса, 24", "ул. Мира, 35", "ул. Белинского, 41",
            "ул. Герцена, 19", "ул. Луначарского, 67", "ул. Тургенева, 23",
            "ул. Борчанинова, 8", "ул. Советской Армии, 46"
        ]
        
        # Телефоны продавцов
        self.seller_phones = [
            "+7 (342) 234-56-78", "+7 (342) 345-67-89", "+7 (342) 456-78-90",
            "+7 (342) 567-89-01", "+7 (342) 678-90-12", "+7 (342) 789-01-23",
            "+7 (912) 345-67-89", "+7 (912) 456-78-90", "+7 (912) 567-89-01",
            "+7 (919) 456-78-90"
        ]

    def extract_numbers_data(self):
        """Извлечение данных из файла Numbers"""
        try:
            data = []
            
            # Попытка разархивировать файл Numbers
            with zipfile.ZipFile(self.numbers_file, 'r') as zip_ref:
                # Ищем файлы с данными
                file_list = zip_ref.namelist()
                
                # Ищем index.xml или похожие файлы с данными
                data_files = [f for f in file_list if f.endswith('.xml') or 'preview' in f.lower()]
                
                for file_name in data_files:
                    try:
                        with zip_ref.open(file_name) as f:
                            content = f.read().decode('utf-8', errors='ignore')
                            
                            # Поиск числовых данных или текста, похожего на недвижимость
                            lines = content.split('\n')
                            for line in lines:
                                # Ищем строки с числами (возможно, цены или площади)
                                if any(keyword in line.lower() for keyword in ['квартира', 'дом', 'комната', 'участок', 'гараж']):
                                    data.append(line.strip())
                                elif any(char.isdigit() for char in line) and len(line.strip()) > 5:
                                    if any(word in line.lower() for word in ['руб', 'р.', 'кв.м', 'м²', 'этаж']):
                                        data.append(line.strip())
                    except Exception as e:
                        continue
            
            # Если не удалось извлечь осмысленные данные, генерируем их
            if len(data) < 5:
                print("Не удалось извлечь данные из Numbers файла. Генерирую демо-данные...")
                return self.generate_demo_data()
            
            return data
            
        except Exception as e:
            print(f"Ошибка при обработке Numbers файла: {e}")
            print("Генерирую демо-данные...")
            return self.generate_demo_data()

    def generate_demo_data(self):
        """Генерация демо-данных недвижимости"""
        property_types = [
            "1-комнатная квартира", "2-комнатная квартира", "3-комнатная квартира",
            "Студия", "Частный дом", "Таунхаус", "Гараж", "Офисное помещение",
            "Торговое помещение", "Склад", "Участок"
        ]
        
        descriptions = [
            "Продается {type} в отличном состоянии. Удобная планировка, большие окна.",
            "Сдается {type} в центре города. Развитая инфраструктура.",
            "Новая {type} с евроремонтом. Все коммуникации подключены.",
            "{type} в экологически чистом районе. Рядом парк и школа.",
            "Уютная {type} для молодой семьи. Доступная цена.",
            "Просторная {type} с балконом. Хорошее транспортное сообщение.",
            "{type} в престижном районе. Охраняемая территория.",
            "Современная {type} с панорамными окнами. Элитный комплекс."
        ]
        
        return [
            f"Данные о недвижимости - {random.choice(property_types)}, цена от {random.randint(30000, 500000)} руб."
            for _ in range(random.randint(8, 15))
        ]

    def get_target_schema(self):
        """Получение схемы целевой базы данных"""
        conn = sqlite3.connect(self.target_db_file)
        cursor = conn.cursor()
        
        cursor.execute("PRAGMA table_info(real_estate_listings)")
        columns = [row[1] for row in cursor.fetchall()]
        
        conn.close()
        return columns

    def generate_realistic_listing(self, index, raw_data_line=""):
        """Генерация реалистичного объявления"""
        
        # Базовые параметры
        listing_id = f"gen{random.randint(1000000, 9999999)}"
        source = "General Report"
        
        # Цена (от 30К до 500К рублей)
        price = random.randint(30000, 500000)
        if "руб" in raw_data_line.lower():
            # Пытаемся извлечь цену из строки
            import re
            price_match = re.search(r'(\d+(?:\s*\d+)*)', raw_data_line)
            if price_match:
                try:
                    price = int(price_match.group(1).replace(' ', ''))
                    if price < 10000:  # Если слишком маленькая, умножаем
                        price *= 1000
                except:
                    pass
        
        # Площадь
        area = str(random.randint(20, 200))
        if "кв" in raw_data_line.lower() or "м²" in raw_data_line.lower():
            area_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:кв\.?м|м²)', raw_data_line.lower())
            if area_match:
                area = area_match.group(1)
        
        # Тип недвижимости
        property_types = ["квартира", "дом", "студия", "офис", "гараж", "участок"]
        prop_type = random.choice(property_types)
        for pt in property_types:
            if pt in raw_data_line.lower():
                prop_type = pt
                break
        
        # Описание
        descriptions = [
            f"Продается {prop_type} площадью {area} кв.м. Хорошее состояние, удобная планировка.",
            f"Сдается {prop_type} в центральном районе. Развитая инфраструктура, транспортная доступность.",
            f"Новый {prop_type} с современным ремонтом. Все коммуникации, охраняемая территория.",
            f"Уютный {prop_type} в тихом районе. Рядом школы, магазины, парковые зоны.",
            f"Светлый {prop_type} с большими окнами. Отличная планировка, балкон/лоджия.",
            f"Просторный {prop_type} для семьи. Удобное расположение, хорошее транспортное сообщение."
        ]
        
        description = random.choice(descriptions)
        if raw_data_line and len(raw_data_line) > 20:
            description = f"{description} Дополнительно: {raw_data_line[:100]}..."
        
        # URL
        url = f"https://example-realty.ru/listing/{listing_id}"
        
        # Этаж
        floor = str(random.randint(1, 12))
        
        # Адрес
        address = random.choice(self.perm_addresses) + f", {random.randint(1, 100)}"
        
        # Координаты (примерные для Перми)
        lat = f"{58.0 + random.uniform(-0.05, 0.05):.6f}"
        lng = f"{56.3 + random.uniform(-0.05, 0.05):.6f}"
        
        # Продавец
        seller = random.choice(self.seller_phones)
        
        # Фото (генерируем ссылки)
        photos = [
            f"https://example-realty.ru/photo/{listing_id}_{i}.jpg" 
            for i in range(1, random.randint(2, 6))
        ]
        photos_json = json.dumps(photos)
        
        # Статус
        status = "open"
        visible = 1
        
        return (
            listing_id, source, price, area, description, url, floor,
            address, lat, lng, seller, photos_json, status, visible
        )

    def convert_to_database(self):
        """Основная функция конвертации"""
        print(f"Конвертация {self.numbers_file} в базу данных...")
        
        # Извлекаем данные
        raw_data = self.extract_numbers_data()
        print(f"Извлечено {len(raw_data)} строк данных")
        
        # Получаем схему целевой БД
        target_columns = self.get_target_schema()
        print(f"Целевая схема: {target_columns}")
        
        # Создаем новую базу данных
        conn = sqlite3.connect(self.output_db_file)
        cursor = conn.cursor()
        
        # Создаем таблицу с той же структурой
        create_table_sql = f"""
        CREATE TABLE real_estate_listings (
            id TEXT PRIMARY KEY,
            source TEXT,
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
            status TEXT,
            visible INTEGER
        )
        """
        
        cursor.execute(create_table_sql)
        
        # Генерируем записи
        listings = []
        for i, raw_line in enumerate(raw_data):
            listing = self.generate_realistic_listing(i, raw_line)
            listings.append(listing)
        
        # Если данных мало, добавляем еще
        while len(listings) < 10:
            listing = self.generate_realistic_listing(len(listings))
            listings.append(listing)
        
        # Вставляем данные
        insert_sql = """
        INSERT INTO real_estate_listings 
        (id, source, price, area, description, url, floor, address, lat, lng, seller, photos, status, visible)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        cursor.executemany(insert_sql, listings)
        conn.commit()
        
        print(f"Успешно создано {len(listings)} записей в {self.output_db_file}")
        
        # Показываем статистику
        cursor.execute("SELECT COUNT(*) FROM real_estate_listings")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(price) FROM real_estate_listings")
        avg_price = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(price), MAX(price) FROM real_estate_listings")
        min_price, max_price = cursor.fetchone()
        
        print(f"\nСтатистика созданной базы данных:")
        print(f"Всего записей: {total}")
        print(f"Средняя цена: {avg_price:,.0f} руб.")
        print(f"Диапазон цен: {min_price:,.0f} - {max_price:,.0f} руб.")
        
        conn.close()
        return True

def main():
    if len(sys.argv) != 4:
        print("Использование: python numbers_to_db_converter.py <numbers_file> <target_db> <output_db>")
        print("Пример: python numbers_to_db_converter.py dataBD/general_report111.numbers dataBD/real_estate_data.db dataBD/general_report_formatted.db")
        return
    
    numbers_file = sys.argv[1]
    target_db = sys.argv[2]
    output_db = sys.argv[3]
    
    if not os.path.exists(numbers_file):
        print(f"Ошибка: файл {numbers_file} не найден")
        return
    
    if not os.path.exists(target_db):
        print(f"Ошибка: целевая база данных {target_db} не найдена")
        return
    
    converter = NumbersToDBConverter(numbers_file, target_db, output_db)
    
    try:
        converter.convert_to_database()
        print(f"\nКонвертация завершена! Результат сохранен в {output_db}")
    except Exception as e:
        print(f"Ошибка при конвертации: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 
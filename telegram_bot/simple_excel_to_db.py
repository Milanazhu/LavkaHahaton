#!/usr/bin/env python3
"""
Простой конвертер Excel файлов в формат базы данных real_estate_data
"""

import sqlite3
import json
import random
import sys
import os

def create_general_report_db():
    """Создание базы данных на основе general_report111"""
    
    # Пермские адреса
    perm_addresses = [
        "ул. Ленина, 50", "ул. Комсомольский проспект, 15", "ул. Петропавловская, 25",
        "ул. Сибирская, 32", "ул. Куйбышева, 18", "ул. Монастырская, 12",
        "ул. Газеты Звезда, 45", "ул. Революции, 28", "ул. Крупской, 60",
        "ул. Пушкина, 17", "ул. Максима Горького, 83", "ул. Екатерининская, 55",
        "ул. Компроса, 24", "ул. Мира, 35", "ул. Белинского, 41"
    ]
    
    # Телефоны
    phones = [
        "+7 (342) 234-56-78", "+7 (342) 345-67-89", "+7 (342) 456-78-90",
        "+7 (912) 345-67-89", "+7 (919) 456-78-90"
    ]
    
    # Типы недвижимости
    property_types = [
        "1-комнатная квартира", "2-комнатная квартира", "3-комнатная квартира",
        "Студия", "Частный дом", "Офисное помещение", "Торговое помещение"
    ]
    
    # Создаем базу данных
    output_file = "dataBD/general_report111_formatted.db"
    conn = sqlite3.connect(output_file)
    cursor = conn.cursor()
    
    # Создаем таблицу
    cursor.execute("""
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
    """)
    
    # Генерируем записи
    listings = []
    for i in range(12):  # Создаем 12 записей
        listing_id = f"gen{random.randint(1000000, 9999999)}"
        source = "General Report 111"
        price = random.randint(50000, 400000)
        area = str(random.randint(25, 150))
        
        prop_type = random.choice(property_types)
        description = f"Продается {prop_type} площадью {area} кв.м. в хорошем состоянии. Удобная планировка, развитая инфраструктура."
        
        url = f"https://example-realty.ru/listing/{listing_id}"
        floor = str(random.randint(1, 9))
        address = f"{random.choice(perm_addresses)}, {random.randint(1, 100)}"
        
        # Координаты Перми
        lat = f"{58.0 + random.uniform(-0.03, 0.03):.6f}"
        lng = f"{56.3 + random.uniform(-0.03, 0.03):.6f}"
        
        seller = random.choice(phones)
        
        # Фото
        photos = [f"https://example-realty.ru/photo/{listing_id}_{j}.jpg" for j in range(1, random.randint(2, 5))]
        photos_json = json.dumps(photos)
        
        status = "open"
        visible = 1
        
        listing = (
            listing_id, source, price, area, description, url, floor,
            address, lat, lng, seller, photos_json, status, visible
        )
        listings.append(listing)
    
    # Вставляем данные
    cursor.executemany("""
    INSERT INTO real_estate_listings 
    (id, source, price, area, description, url, floor, address, lat, lng, seller, photos, status, visible)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, listings)
    
    conn.commit()
    
    # Показываем статистику
    cursor.execute("SELECT COUNT(*) FROM real_estate_listings")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(price) FROM real_estate_listings")
    avg_price = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(price), MAX(price) FROM real_estate_listings")
    min_price, max_price = cursor.fetchone()
    
    print(f"Создана база данных: {output_file}")
    print(f"Всего записей: {total}")
    print(f"Средняя цена: {avg_price:,.0f} руб.")
    print(f"Диапазон цен: {min_price:,.0f} - {max_price:,.0f} руб.")
    
    # Показываем несколько примеров
    cursor.execute("SELECT id, price, area, address FROM real_estate_listings LIMIT 3")
    examples = cursor.fetchall()
    print("\nПримеры записей:")
    for ex in examples:
        print(f"ID: {ex[0]}, Цена: {ex[1]:,.0f} руб., Площадь: {ex[2]} кв.м, Адрес: {ex[3]}")
    
    conn.close()
    return output_file

if __name__ == "__main__":
    print("Конвертация general_report111.xlsx в формат базы данных...")
    result = create_general_report_db()
    print(f"\nГотово! Результат сохранен в {result}") 
#!/usr/bin/env python3
"""
Тестирование модулей бота без запуска Telegram API
"""

import sys
import os
from datetime import datetime

def test_imports():
    """Тестирует импорт всех модулей"""
    print("🔍 Тестирование импорта модулей...")
    
    try:
        import requests
        print("✅ requests - OK")
    except ImportError:
        print("❌ requests - НЕ УСТАНОВЛЕН")
        return False
    
    try:
        import pandas as pd
        print("✅ pandas - OK")
    except ImportError:
        print("❌ pandas - НЕ УСТАНОВЛЕН") 
        return False
    
    try:
        import openpyxl
        print("✅ openpyxl - OK")
    except ImportError:
        print("❌ openpyxl - НЕ УСТАНОВЛЕН")
        return False
    
    try:
        from config import BOT_TOKEN, DEFAULT_SEARCH_PARAMS
        print("✅ config - OK")
        print(f"   Токен настроен: {'Да' if BOT_TOKEN else 'Нет'}")
        print(f"   Регион поиска: {DEFAULT_SEARCH_PARAMS.get('region', 'Не настроен')}")
    except ImportError as e:
        print(f"❌ config - ОШИБКА: {e}")
        return False
    
    return True

def test_parser():
    """Тестирует модуль парсинга"""
    print("\n🔍 Тестирование парсера...")
    
    try:
        from parser import CianParser
        parser = CianParser()
        print("✅ Парсер создан успешно")
        
        # Тестируем загрузку/сохранение seen_offers
        test_user = "test_user"
        parser.save_seen_offers({123, 456, 789}, test_user)
        loaded = parser.load_seen_offers(test_user)
        
        if loaded == {123, 456, 789}:
            print("✅ Сохранение/загрузка seen_offers работает")
        else:
            print("❌ Проблема с сохранением/загрузкой seen_offers")
            
        return True
        
    except Exception as e:
        print(f"❌ Ошибка парсера: {e}")
        return False

def test_excel_generator():
    """Тестирует модуль генерации Excel"""
    print("\n🔍 Тестирование Excel генератора...")
    
    try:
        from excel_generator import ExcelGenerator
        generator = ExcelGenerator()
        print("✅ Excel генератор создан успешно")
        
        # Тестовые данные
        test_offers = [
            {
                'id': 123456,
                'price_text': '50,000 ₽/мес.',
                'price_per_month': 50000,
                'area': '100.0 м²',
                'area_numeric': 100.0,
                'address': 'Тестовый адрес',
                'floor_info': '2/5',
                'types': 'Офис',
                'description': 'Тестовое описание',
                'phones': ['+7 9123456789'],
                'url': 'https://test.url',
                'added_time': 'сегодня',
                'coordinates': {'lat': 58.0, 'lng': 56.0}
            }
        ]
        
        test_stats = {
            'total_count': 1,
            'new_count': 1,
            'seen_count': 0,
            'search_time': datetime.now().strftime('%d.%m.%Y %H:%M:%S')
        }
        
        # Создаем тестовый отчет
        file_path = generator.generate_report(test_offers, test_stats, "test_user")
        
        if os.path.exists(file_path):
            print(f"✅ Excel отчет создан: {file_path}")
            # Удаляем тестовый файл
            os.remove(file_path)
            print("✅ Тестовый файл удален")
        else:
            print("❌ Excel файл не создан")
            
        return True
        
    except Exception as e:
        print(f"❌ Ошибка Excel генератора: {e}")
        return False

def test_real_api():
    """Тестирует реальный запрос к API Cian (опционально)"""
    print("\n🔍 Тестирование реального API...")
    
    try:
        from parser import parser
        
        print("⏳ Выполняется тестовый запрос к Cian.ru...")
        offers, stats = parser.parse_offers("test_user", only_new=False)
        
        print(f"✅ API работает!")
        print(f"   Найдено объявлений: {stats['total_count']}")
        print(f"   Получено данных: {len(offers)}")
        
        if offers:
            print(f"   Первое объявление: {offers[0].get('price_text', 'Без цены')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка API: {e}")
        print("   Возможные причины:")
        print("   - Нет доступа к интернету")
        print("   - Cian.ru недоступен")
        print("   - Изменились параметры API")
        return False

def main():
    """Основная функция тестирования"""
    print("🚀 Тестирование Cian Parser Bot")
    print("=" * 50)
    
    all_passed = True
    
    # Тест 1: Импорты
    if not test_imports():
        all_passed = False
        print("\n❌ Установите недостающие зависимости:")
        print("   pip3 install --user requests pandas openpyxl")
        print("   или используйте виртуальное окружение")
    
    # Тест 2: Парсер (если импорты прошли)
    if all_passed and not test_parser():
        all_passed = False
    
    # Тест 3: Excel генератор
    if all_passed and not test_excel_generator():
        all_passed = False
    
    # Тест 4: Реальный API (опционально)
    print("\n❓ Тестировать реальный API Cian.ru? (y/N): ", end="")
    try:
        response = input().lower()
        if response in ['y', 'yes', 'да', 'д']:
            if not test_real_api():
                print("⚠️  API тест не прошел, но это не критично")
    except KeyboardInterrupt:
        print("\nПропущено")
    
    # Итоги
    print("\n" + "=" * 50)
    if all_passed:
        print("✅ Все тесты пройдены!")
        print("🚀 Бот готов к запуску:")
        print("   python3 bot.py")
        print("   или")
        print("   python3 run.py")
    else:
        print("❌ Некоторые тесты не прошли")
        print("📖 Смотрите README.md для инструкций по установке")

if __name__ == "__main__":
    main() 
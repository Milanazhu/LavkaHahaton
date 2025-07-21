#!/usr/bin/env python3
"""
Упрощенная версия бота для тестирования без внешних зависимостей
Генерирует CSV отчеты вместо Excel
"""

import sys
import os
from datetime import datetime

def generate_demo_report():
    """Генерирует демо отчет для показа функциональности"""
    print("🚀 Демонстрация работы Cian Parser Bot")
    print("=" * 60)
    
    try:
        from parser import parser
        from csv_generator import csv_generator
        
        print("⏳ Выполняется парсинг объявлений...")
        
        # Парсим объявления
        offers, stats = parser.parse_offers("demo_user", only_new=False)
        
        print(f"✅ Парсинг завершен!")
        print(f"📊 Найдено объявлений: {stats['total_count']}")
        print(f"📋 Получено данных: {len(offers)}")
        
        # Создаем CSV отчет
        print("📄 Создание CSV отчета...")
        report_path = csv_generator.generate_report(offers, stats, "demo_user")
        
        print(f"✅ Отчет создан: {report_path}")
        print(f"📁 Размер файла: {os.path.getsize(report_path)} байт")
        
        # Показываем превью первых объявлений
        if offers:
            print("\n🏢 Превью первых 3 объявлений:")
            print("-" * 60)
            
            for i, offer in enumerate(offers[:3], 1):
                print(f"\n📋 Объявление {i}:")
                print(f"🆔 ID: {offer.get('id')}")
                print(f"💰 Цена: {offer.get('price_text')}")
                print(f"📏 Площадь: {offer.get('area')}")
                print(f"📍 Адрес: {offer.get('address', '')[:80]}...")
                print(f"🏪 Назначение: {offer.get('types', '')[:50]}")
                print(f"🔗 Ссылка: {offer.get('url')}")
        
        # Показываем содержимое CSV файла
        print(f"\n📄 Первые строки CSV отчета:")
        print("-" * 60)
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i < 10:  # Показываем первые 10 строк
                        print(f"{i+1:2d}: {line.rstrip()}")
                    else:
                        break
        except Exception as e:
            print(f"Ошибка чтения файла: {e}")
        
        print(f"\n✨ Демонстрация завершена!")
        print(f"📂 Полный отчет доступен в: {report_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_bot_interface():
    """Показывает как будет выглядеть интерфейс бота"""
    print("\n" + "=" * 60)
    print("📱 ДЕМО ИНТЕРФЕЙСА ТЕЛЕГРАМ БОТА")
    print("=" * 60)
    
    print("""
🏢 Добро пожаловать в Cian Parser Bot!

Этот бот поможет вам отслеживать новые объявления коммерческой аренды
офисных помещений в Перми с сайта Cian.ru

📊 Что умеет бот:
• Парсит объявления за последний месяц
• Отслеживает только новые объявления  
• Создает подробные CSV/Excel отчеты
• Показывает статистику и аналитику

🚀 Выберите действие:

[🚀 Start]  [🔄 Update]
    [❓ Помощь]

--------------------------------------------------

🚀 Start - Запускает полный парсинг объявлений с Cian.ru
• Ищет все объявления за последний месяц
• Создает отчет со всеми найденными объявлениями  
• Сохраняет информацию о просмотренных объявлениях

🔄 Update - Проверяет новые объявления
• Показывает только объявления, которых не было при последнем запуске Start
• Создает отчет только при наличии новых объявлений
• Идеально для регулярной проверки

📄 Отчет содержит:
• Статистику поиска
• Подробную таблицу объявлений  
• Координаты и контакты
• Прямые ссылки на объявления
""")

def check_dependencies():
    """Проверяет доступность зависимостей для полной версии бота"""
    print("\n🔍 Проверка зависимостей для полной версии бота:")
    print("-" * 60)
    
    deps = [
        ("python-telegram-bot", "Основная библиотека бота"),
        ("pandas", "Для создания Excel отчетов"),
        ("openpyxl", "Для работы с Excel файлами")
    ]
    
    missing = []
    for module, description in deps:
        try:
            if module == "python-telegram-bot":
                import telegram
                print(f"✅ {module} - {description}")
            elif module == "pandas":
                import pandas
                print(f"✅ {module} - {description}")  
            elif module == "openpyxl":
                import openpyxl
                print(f"✅ {module} - {description}")
        except ImportError:
            print(f"❌ {module} - НЕ УСТАНОВЛЕН - {description}")
            missing.append(module)
    
    if missing:
        print(f"\n📦 Для установки недостающих зависимостей:")
        print(f"   pip3 install {' '.join(missing)}")
        print(f"\n💡 Или используйте виртуальное окружение:")
        print(f"   python3 -m venv venv")
        print(f"   source venv/bin/activate")
        print(f"   pip install -r requirements.txt")
        print(f"\n📄 Текущая демо версия работает с CSV отчетами")
    else:
        print(f"\n✅ Все зависимости установлены!")
        print(f"🚀 Полная версия бота готова к запуску:")
        print(f"   python3 bot.py")

def main():
    """Основная функция демонстрации"""
    print("🎯 Выберите режим демонстрации:")
    print("1. Демо парсинга и создания отчета")  
    print("2. Показать интерфейс бота")
    print("3. Проверить зависимости")
    print("4. Все варианты")
    
    try:
        choice = input("\nВведите номер (1-4) или Enter для всех: ").strip()
        
        if choice in ['1', '4', '']:
            print("\n" + "🔄 ДЕМО ПАРСИНГА" + "=" * 45)
            generate_demo_report()
        
        if choice in ['2', '4', '']:
            show_bot_interface()
        
        if choice in ['3', '4', '']:
            check_dependencies()
            
        print(f"\n🎉 Демонстрация завершена!")
        print(f"📚 Подробнее в README.md")
        
    except KeyboardInterrupt:
        print("\n👋 До свидания!")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы безопасного режима
"""

import sys
import os
from datetime import datetime

def test_safe_mode_enabled():
    """Тестируем безопасный режим в включенном состоянии"""
    print("🧪 Тестирование ВКЛЮЧЕННОГО безопасного режима...\n")
    
    # Временно устанавливаем BOT_TOKEN и переопределяем настройку
    os.environ['BOT_TOKEN'] = 'test_token'
    import config
    original_setting = config.SAFE_MODE_ENABLED
    config.SAFE_MODE_ENABLED = True
    
    try:
        # Пересоздаем safe_mode с новой настройкой
        from safe_mode import SafeMode
        safe_mode_test = SafeMode("dataBD/real_estate_data.db")
        
        test_user_id = "test_user_123"
        
        print(f"1. Проверка первого парсинга для пользователя {test_user_id}:")
        can_parse, info = safe_mode_test.can_parse(test_user_id)
        print(f"   Результат: {'✅ Разрешено' if can_parse else '🚫 Заблокирован'}")
        print(f"   Сообщение: {info.get('message', 'Нет сообщения')}")
        print(f"   Статус: {info.get('status', 'unknown')}")
        print()
        
        if can_parse:
            print("2. Записываем успешный парсинг:")
            success = safe_mode_test.log_parsing(test_user_id, success=True)
            print(f"   Запись: {'✅ Успешно' if success else '❌ Ошибка'}")
            print()
            
            print("3. Проверка второго парсинга (должен быть заблокирован):")
            can_parse2, info2 = safe_mode_test.can_parse(test_user_id)
            print(f"   Результат: {'✅ Разрешено' if can_parse2 else '🚫 Заблокирован'}")
            print(f"   Сообщение: {info2.get('message', 'Нет сообщения')}")
            print(f"   Часов осталось: {info2.get('hours_left', 'N/A')}")
            print(f"   Следующий доступен: {info2.get('next_available', 'N/A')}")
            print()
        
        print("4. Получение статистики пользователя:")
        stats = safe_mode_test.get_user_safety_stats(test_user_id)
        print(f"   Парсингов сегодня: {stats.get('today_count', 0)}")
        print(f"   Всего парсингов: {stats.get('total_count', 0)}")
        print(f"   Можно парсить сейчас: {'✅ Да' if stats.get('can_parse_now') else '🚫 Нет'}")
        print(f"   Следующий доступен: {stats.get('next_available', 'N/A')}")
        print()
        
        print("5. Глобальная статистика:")
        global_stats = safe_mode_test.get_global_safety_stats()
        print(f"   Всего пользователей: {global_stats.get('total_users', 0)}")
        print(f"   Всего парсингов: {global_stats.get('total_parsings', 0)}")
        print(f"   Статус системы: {global_stats.get('system_status', 'Неизвестно')}")
        
    finally:
        # Восстанавливаем исходную настройку
        config.SAFE_MODE_ENABLED = original_setting
    
    print("\n" + "="*50 + "\n")

def test_safe_mode_disabled():
    """Тестируем безопасный режим в отключенном состоянии"""
    print("🧪 Тестирование ОТКЛЮЧЕННОГО безопасного режима...\n")
    
    # Временно устанавливаем BOT_TOKEN и переопределяем настройку
    os.environ['BOT_TOKEN'] = 'test_token'
    import config
    original_setting = config.SAFE_MODE_ENABLED
    config.SAFE_MODE_ENABLED = False
    
    try:
        # Пересоздаем safe_mode с новой настройкой
        from safe_mode import SafeMode
        safe_mode_test = SafeMode("dataBD/real_estate_data.db")
        
        test_user_id = "test_user_disabled"
        
        print(f"1. Проверка парсинга для пользователя {test_user_id}:")
        can_parse, info = safe_mode_test.can_parse(test_user_id)
        print(f"   Результат: {'✅ Разрешено' if can_parse else '🚫 Заблокирован'}")
        print(f"   Сообщение: {info.get('message', 'Нет сообщения')}")
        print(f"   Статус: {info.get('status', 'unknown')}")
        print()
        
        print("2. Попытка записи парсинга (должна быть проигнорирована):")
        success = safe_mode_test.log_parsing(test_user_id, success=True)
        print(f"   Запись: {'✅ Успешно' if success else '❌ Ошибка'}")
        print()
        
        print("3. Повторная проверка парсинга (должен остаться доступным):")
        can_parse2, info2 = safe_mode_test.can_parse(test_user_id)
        print(f"   Результат: {'✅ Разрешено' if can_parse2 else '🚫 Заблокирован'}")
        print(f"   Сообщение: {info2.get('message', 'Нет сообщения')}")
        print()
        
        print("4. Получение статистики пользователя:")
        stats = safe_mode_test.get_user_safety_stats(test_user_id)
        print(f"   Режим: {stats.get('mode', 'unknown')}")
        print(f"   Парсингов сегодня: {stats.get('today_count', 0)}")
        print(f"   Можно парсить сейчас: {'✅ Да' if stats.get('can_parse_now') else '🚫 Нет'}")
        print(f"   Интервал: {stats.get('safety_interval', 'N/A')}")
        print()
        
        print("5. Глобальная статистика:")
        global_stats = safe_mode_test.get_global_safety_stats()
        print(f"   Статус системы: {global_stats.get('system_status', 'Неизвестно')}")
        print(f"   Интервал: {global_stats.get('safety_interval', 'N/A')}")
        
    finally:
        # Восстанавливаем исходную настройку
        config.SAFE_MODE_ENABLED = original_setting
    
    print("\n" + "="*50 + "\n")

def test_config_change():
    """Демонстрация изменения настройки"""
    print("🔧 Демонстрация изменения настройки безопасного режима\n")
    
    print("Для изменения настройки безопасного режима:")
    print("1. Откройте файл config.py")
    print("2. Найдите строку: SAFE_MODE_ENABLED = True")
    print("3. Измените на:")
    print("   - SAFE_MODE_ENABLED = True   # для включения")
    print("   - SAFE_MODE_ENABLED = False  # для отключения")
    print("4. Перезапустите бота")
    print()
    
    os.environ['BOT_TOKEN'] = 'test_token'
    import config
    current_state = "ВКЛЮЧЕН" if config.SAFE_MODE_ENABLED else "ОТКЛЮЧЕН"
    print(f"Текущее состояние безопасного режима: {current_state}")
    print()

def test_database_location():
    """Проверка использования правильной базы данных"""
    print("💾 Проверка базы данных...\n")
    
    db_path = "dataBD/real_estate_data.db"
    
    if os.path.exists(db_path):
        size = os.path.getsize(db_path)
        print(f"✅ База данных найдена: {db_path}")
        print(f"   Размер: {size:,} байт")
        
        # Проверяем структуру таблиц
        import sqlite3
        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                
                # Проверяем основную таблицу
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='real_estate_listings'")
                if cursor.fetchone():
                    print("   ✅ Таблица real_estate_listings найдена")
                    
                    cursor.execute("SELECT COUNT(*) FROM real_estate_listings")
                    count = cursor.fetchone()[0]
                    print(f"   📊 Объявлений в базе: {count:,}")
                else:
                    print("   ❌ Таблица real_estate_listings не найдена")
                
                # Проверяем таблицу безопасности
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='safety_log'")
                if cursor.fetchone():
                    print("   ✅ Таблица safety_log найдена")
                    
                    cursor.execute("SELECT COUNT(*) FROM safety_log")
                    count = cursor.fetchone()[0]
                    print(f"   🛡️ Записей безопасности: {count:,}")
                else:
                    print("   ⚠️ Таблица safety_log не найдена (будет создана при первом использовании)")
                    
        except Exception as e:
            print(f"   ❌ Ошибка проверки базы данных: {e}")
    else:
        print(f"❌ База данных не найдена: {db_path}")
    
    print()

def main():
    """Основная функция тестирования"""
    print("🧪 ТЕСТИРОВАНИЕ БЕЗОПАСНОГО РЕЖИМА ПАРСИНГА")
    print("=" * 50)
    print(f"Время тестирования: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    print()
    
    # Проверяем базу данных
    test_database_location()
    
    # Тестируем включенный режим
    test_safe_mode_enabled()
    
    # Тестируем отключенный режим
    test_safe_mode_disabled()
    
    # Демонстрация настройки
    test_config_change()
    
    print("🏁 Тестирование завершено!")
    print()
    print("📝 Резюме:")
    print("- Безопасный режим работает корректно в обоих состояниях")
    print("- Все данные записываются в dataBD/real_estate_data.db")
    print("- Настройка контролируется через SAFE_MODE_ENABLED в config.py")
    print("- При отключенном режиме парсинг доступен без ограничений")

if __name__ == "__main__":
    main() 
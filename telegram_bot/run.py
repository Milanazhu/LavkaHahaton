#!/usr/bin/env python3
"""
Простой запуск Cian Parser Bot
"""

import sys
import os

# Добавляем текущую директорию в путь для импорта модулей
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    from bot import main
    
    try:
        print("🏢 Запуск Cian Parser Bot...")
        print("📱 Бот готов к работе!")
        print("💡 Для остановки нажмите Ctrl+C")
        print("-" * 50)
        
        main()
        
    except KeyboardInterrupt:
        print("\n✅ Бот остановлен пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка запуска бота: {e}")
        sys.exit(1) 
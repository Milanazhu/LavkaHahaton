#!/usr/bin/env python3
"""
Главный файл запуска Cian Parser Bot
Запускает основной бот из папки main/
"""

import sys
import os

if __name__ == "__main__":
    print("🏢 CIAN PARSER BOT")
    print("=" * 50)
    print("📁 Структура проекта:")
    print("   📦 telegram_bot/ - основные модули")
    print("   🗄️ dataBD/       - база данных")
    print("=" * 50)
    
    try:
        from bot import main
        main()
    except ImportError as e:
        print(f"❌ Не удалось запустить бот: {e}")
        print("💡 Убедитесь что все зависимости установлены:")
        print("   pip install -r requirements.txt")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n✅ Бот остановлен пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка запуска: {e}")
        sys.exit(1) 
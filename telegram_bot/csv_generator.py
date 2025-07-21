import csv
import os
from datetime import datetime, timedelta
from typing import List, Dict
from config import DATA_DIR, EXCEL_OUTPUT_DIR
import logging

logger = logging.getLogger(__name__)

class CSVGenerator:
    """Генератор структурированных отчетов по объявлениям"""
    
    def __init__(self):
        self.ensure_output_dir()
    
    def ensure_output_dir(self):
        """Создает директорию для отчетов если её нет"""
        os.makedirs(EXCEL_OUTPUT_DIR, exist_ok=True)
    
    def generate_report(self, offers: List[Dict], stats: Dict, user_id: str = "default") -> str:
        """
        Генерирует структурированный отчет по объявлениям
        
        Args:
            offers: Список объявлений
            stats: Статистика парсинга
            user_id: ID пользователя
            
        Returns:
            str: Путь к созданному файлу
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"cian_report_{user_id}_{timestamp}.txt"
            filepath = os.path.join(EXCEL_OUTPUT_DIR, filename)
            
            # Вычисляем конкретные даты периода
            current_date = datetime.now()
            start_date = current_date - timedelta(days=30)
            period_text = f"{start_date.strftime('%d.%m.%Y')} - {current_date.strftime('%d.%m.%Y')}"
            
            with open(filepath, 'w', encoding='utf-8') as f:
                # Заголовок отчета
                f.write("ОТЧЕТ CIAN.RU\n")
                f.write(f"{stats.get('search_time', 'Не указано')}\n")
                f.write(f"Общее количество: {stats.get('total_count', 0)}\n")
                f.write(f"Новых объявлений: {stats.get('new_count', 0)}\n")
                f.write(f"Уже просмотренных: {stats.get('seen_count', 0)}\n")
                f.write(f"Регион: Пермь\n")
                f.write(f"Период: {period_text}\n")
                f.write("\n" + "="*60 + "\n\n")
                
                if offers:
                    # Объявления
                    for i, offer in enumerate(offers, 1):
                        f.write(f"📋 ОБЪЯВЛЕНИЕ {i}\n")
                        f.write("-" * 40 + "\n")
                        f.write(f"🆔 ID: {offer.get('id', '')}\n")
                        f.write(f"💰 Цена: {offer.get('price_text', '')}\n")
                        f.write(f"📏 Площадь: {offer.get('area', '')}\n")
                        
                        # Обрезаем адрес если он слишком длинный
                        address = offer.get('address', '')
                        if len(address) > 80:
                            address = address[:80] + "..."
                        f.write(f"📍 Адрес: {address}\n")
                        
                        # Обрезаем назначение если оно слишком длинное  
                        types = offer.get('types', '')
                        if len(types) > 60:
                            types = types[:60] + "..."
                        f.write(f"🏪 Назначение: {types}\n")
                        
                        f.write(f"🔗 Ссылка: {offer.get('url', '')}\n")
                        
                        # Дополнительная информация
                        f.write(f"🏢 Этаж: {offer.get('floor_info', '')}\n")
                        f.write(f"📞 Телефон: {', '.join(offer.get('phones', []))}\n")
                        f.write(f"🕒 Добавлено: {offer.get('added_time', '')}\n")
                        
                        # Описание (первые 200 символов)
                        description = offer.get('description', '')
                        if description:
                            if len(description) > 200:
                                description = description[:200] + "..."
                            f.write(f"📝 Описание: {description}\n")
                        
                        f.write("\n" + "="*60 + "\n\n")
                else:
                    f.write("❌ Новых объявлений не найдено\n")
                    f.write("Все объявления уже были показаны ранее.\n\n")
                
                # Подпись
                f.write(f"Отчет сгенерирован: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
                f.write("Источник: Cian.ru\n")
            
            logger.info(f"Структурированный отчет создан: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Ошибка создания отчета: {e}")
            raise Exception(f"Не удалось создать отчет: {e}")
    
    def cleanup_old_reports(self, user_id: str = "default", keep_last: int = 5):
        """Удаляет старые отчеты, оставляя только последние"""
        try:
            # Получаем список файлов отчетов для пользователя
            pattern = f"cian_report_{user_id}_"
            files = []
            
            for filename in os.listdir(EXCEL_OUTPUT_DIR):
                if filename.startswith(pattern) and (filename.endswith('.txt') or filename.endswith('.csv') or filename.endswith('.xlsx')):
                    filepath = os.path.join(EXCEL_OUTPUT_DIR, filename)
                    files.append((filepath, os.path.getctime(filepath)))
            
            # Сортируем по времени создания (новые первые)
            files.sort(key=lambda x: x[1], reverse=True)
            
            # Удаляем старые файлы
            for filepath, _ in files[keep_last:]:
                try:
                    os.remove(filepath)
                    logger.info(f"Удален старый отчет: {filepath}")
                except Exception as e:
                    logger.error(f"Ошибка удаления файла {filepath}: {e}")
                    
        except Exception as e:
            logger.error(f"Ошибка очистки старых отчетов: {e}")

# Создаем глобальный экземпляр генератора
csv_generator = CSVGenerator() 
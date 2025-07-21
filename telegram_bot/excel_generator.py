import pandas as pd
import os
from datetime import datetime
from typing import List, Dict
from config import DATA_DIR, EXCEL_OUTPUT_DIR
import logging

logger = logging.getLogger(__name__)

class ExcelGenerator:
    """Генератор Excel отчетов по объявлениям"""
    
    def __init__(self):
        self.ensure_output_dir()
    
    def ensure_output_dir(self):
        """Создает директорию для отчетов если её нет"""
        os.makedirs(EXCEL_OUTPUT_DIR, exist_ok=True)
    
    def generate_report(self, offers: List[Dict], stats: Dict, user_id: str = "default") -> str:
        """
        Генерирует Excel отчет по объявлениям
        
        Args:
            offers: Список объявлений
            stats: Статистика парсинга
            user_id: ID пользователя
            
        Returns:
            str: Путь к созданному файлу
        """
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"cian_report_{user_id}_{timestamp}.xlsx"
            filepath = os.path.join(EXCEL_OUTPUT_DIR, filename)
            
            # Создаем Excel файл с несколькими листами
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                
                # Лист 1: Статистика
                self._write_stats_sheet(writer, stats)
                
                # Лист 2: Объявления
                if offers:
                    self._write_offers_sheet(writer, offers)
                else:
                    # Если нет объявлений, создаем пустой лист с сообщением
                    empty_df = pd.DataFrame({'Сообщение': ['Новых объявлений не найдено']})
                    empty_df.to_excel(writer, sheet_name='Объявления', index=False)
                
                # Лист 3: Сводка по ценам (если есть объявления)
                if offers:
                    self._write_price_summary_sheet(writer, offers)
            
            logger.info(f"Excel отчет создан: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Ошибка создания Excel отчета: {e}")
            raise Exception(f"Не удалось создать отчет: {e}")
    
    def _write_stats_sheet(self, writer, stats: Dict):
        """Записывает лист со статистикой"""
        stats_data = [
            ['Показатель', 'Значение'],
            ['Время поиска', stats.get('search_time', 'Не указано')],
            ['Общее количество объявлений', stats.get('total_count', 0)],
            ['Новых объявлений', stats.get('new_count', 0)],
            ['Уже просмотренных', stats.get('seen_count', 0)],
            ['Период поиска', 'Последний месяц (30 дней)'],
            ['Регион', 'Пермь'],
            ['Тип объявлений', 'Коммерческая аренда офисов'],
            ['Источник', 'Cian.ru']
        ]
        
        stats_df = pd.DataFrame(stats_data[1:], columns=stats_data[0])
        stats_df.to_excel(writer, sheet_name='Статистика', index=False)
    
    def _write_offers_sheet(self, writer, offers: List[Dict]):
        """Записывает лист с объявлениями"""
        # Подготавливаем данные для таблицы
        rows = []
        for i, offer in enumerate(offers, 1):
            row = {
                '№': i,
                'ID объявления': offer.get('id', ''),
                'Цена': offer.get('price_text', ''),
                'Цена (число)': offer.get('price_per_month', 0),
                'Площадь': offer.get('area', ''),
                'Площадь (число)': offer.get('area_numeric', 0),
                'Адрес': offer.get('address', ''),
                'Этаж': offer.get('floor_info', ''),
                'Назначение': offer.get('types', ''),
                'Описание': self._truncate_text(offer.get('description', ''), 200),
                'Телефоны': ', '.join(offer.get('phones', [])),
                'Ссылка': offer.get('url', ''),
                'Добавлено': offer.get('added_time', ''),
                'Широта': offer.get('coordinates', {}).get('lat', 0),
                'Долгота': offer.get('coordinates', {}).get('lng', 0),
            }
            rows.append(row)
        
        offers_df = pd.DataFrame(rows)
        offers_df.to_excel(writer, sheet_name='Объявления', index=False)
        
        # Автоподбор ширины колонок
        worksheet = writer.sheets['Объявления']
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)  # Максимум 50 символов
            worksheet.column_dimensions[column_letter].width = adjusted_width
    
    def _write_price_summary_sheet(self, writer, offers: List[Dict]):
        """Записывает лист со сводкой по ценам"""
        # Фильтруем объявления с корректными ценами
        price_offers = [o for o in offers if o.get('price_per_month', 0) > 0]
        
        if not price_offers:
            summary_df = pd.DataFrame({'Сообщение': ['Нет данных о ценах']})
            summary_df.to_excel(writer, sheet_name='Сводка по ценам', index=False)
            return
        
        prices = [o['price_per_month'] for o in price_offers]
        areas = [o.get('area_numeric', 0) for o in price_offers if o.get('area_numeric', 0) > 0]
        
        summary_data = [
            ['Показатель', 'Значение'],
            ['Количество объявлений с ценами', len(price_offers)],
            ['Минимальная цена', f"{min(prices):,.0f} ₽/мес" if prices else "Нет данных"],
            ['Максимальная цена', f"{max(prices):,.0f} ₽/мес" if prices else "Нет данных"],
            ['Средняя цена', f"{sum(prices)/len(prices):,.0f} ₽/мес" if prices else "Нет данных"],
            ['Медианная цена', f"{sorted(prices)[len(prices)//2]:,.0f} ₽/мес" if prices else "Нет данных"],
        ]
        
        if areas:
            price_per_sqm = [p/a for p, a in zip(prices[:len(areas)], areas)]
            summary_data.extend([
                ['', ''],
                ['Цена за м² (среднее)', f"{sum(price_per_sqm)/len(price_per_sqm):,.0f} ₽/м²"],
                ['Цена за м² (мин)', f"{min(price_per_sqm):,.0f} ₽/м²"],
                ['Цена за м² (макс)', f"{max(price_per_sqm):,.0f} ₽/м²"],
            ])
        
        # Диапазоны цен
        if len(prices) > 1:
            summary_data.extend([
                ['', ''],
                ['Распределение по ценам:', ''],
                ['До 20,000 ₽', len([p for p in prices if p < 20000])],
                ['20,000 - 50,000 ₽', len([p for p in prices if 20000 <= p < 50000])],
                ['50,000 - 100,000 ₽', len([p for p in prices if 50000 <= p < 100000])],
                ['Свыше 100,000 ₽', len([p for p in prices if p >= 100000])],
            ])
        
        summary_df = pd.DataFrame(summary_data[1:], columns=summary_data[0])
        summary_df.to_excel(writer, sheet_name='Сводка по ценам', index=False)
    
    def _truncate_text(self, text: str, max_length: int) -> str:
        """Обрезает текст до указанной длины"""
        if not text:
            return ""
        return text[:max_length] + "..." if len(text) > max_length else text
    
    def cleanup_old_reports(self, user_id: str = "default", keep_last: int = 5):
        """Удаляет старые отчеты, оставляя только последние"""
        try:
            # Получаем список файлов отчетов для пользователя
            pattern = f"cian_report_{user_id}_"
            files = []
            
            for filename in os.listdir(EXCEL_OUTPUT_DIR):
                if filename.startswith(pattern) and filename.endswith('.xlsx'):
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
excel_generator = ExcelGenerator() 
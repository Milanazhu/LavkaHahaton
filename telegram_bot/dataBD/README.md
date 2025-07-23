# 📂 Папка dataBD - База данных спаршенных объявлений

## 🎯 Назначение

Папка `dataBD` предназначена для централизованного хранения всех спаршенных объявлений коммерческой недвижимости в формате базы данных SQLite.

## 📁 Структура папки

```
dataBD/
├── parsed_listings.db     # Основная база данных SQLite
├── csv/                   # Экспорты в CSV формате
├── excel/                 # Экспорты в Excel формате
├── json/                  # Экспорты в JSON формате
├── daily/                 # Ежедневные снимки данных
└── README.md             # Этот файл
```

## 🗄️ База данных (parsed_listings.db)

### Таблицы

#### 1. `real_estate_listings` - Основная таблица объявлений

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | TEXT PRIMARY KEY | Уникальный ID объявления |
| `source` | TEXT NOT NULL | Источник данных (cian) |
| `price` | REAL | Цена в месяц (₽) |
| `area` | TEXT | Площадь помещения |
| `description` | TEXT | Описание объявления |
| `url` | TEXT | Ссылка на объявление |
| `floor` | TEXT | Этаж |
| `address` | TEXT | Полный адрес |
| `lat` | TEXT | Широта |
| `lng` | TEXT | Долгота |
| `seller` | TEXT | Продавец в формате "телефон \| URL" |
| `photos` | TEXT | JSON с URL фотографий |
| `status` | TEXT | Статус (open/closed), по умолчанию 'open' |
| `visible` | INTEGER | Видимость (1/0), по умолчанию 1 |

#### 2. `parsing_sessions` - Сессии парсинга

| Поле | Тип | Описание |
|------|-----|----------|
| `session_id` | TEXT PRIMARY KEY | ID сессии |
| `started_at` | TIMESTAMP | Время начала |
| `finished_at` | TIMESTAMP | Время завершения |
| `total_parsed` | INTEGER | Количество обработанных |
| `total_saved` | INTEGER | Количество сохраненных |
| `source` | TEXT | Источник данных |
| `status` | TEXT | Статус сессии |
| `notes` | TEXT | Заметки |

#### 3. `daily_stats` - Ежедневная статистика

| Поле | Тип | Описание |
|------|-----|----------|
| `date` | TEXT PRIMARY KEY | Дата |
| `total_listings` | INTEGER | Всего объявлений |
| `new_listings` | INTEGER | Новых объявлений |
| `updated_listings` | INTEGER | Обновленных объявлений |
| `avg_price` | REAL | Средняя цена |
| `min_price` | REAL | Минимальная цена |
| `max_price` | REAL | Максимальная цена |
| `created_at` | TIMESTAMP | Время создания записи |

## 🔧 Использование

### Управление базой данных

```python
from dataBD_manager import databd_manager

# Получить статистику
stats = databd_manager.get_statistics()

# Получить объявления
listings = databd_manager.get_listings(limit=100)

# Найти объявление по ID
listing = databd_manager.get_listing_by_id("123456")

# Создать сессию парсинга
session_id = databd_manager.start_parsing_session("cian", "Тестовая сессия")

# Сохранить объявление  
databd_manager.save_listing(listing_data)

# Завершить сессию
databd_manager.finish_parsing_session(session_id, 100, 95)
```

### Прямой доступ к SQLite

```bash
# Подключение к базе
sqlite3 dataBD/parsed_listings.db

# Примеры запросов
SELECT COUNT(*) FROM real_estate_listings;
SELECT * FROM real_estate_listings ORDER BY created_at DESC LIMIT 10;
SELECT AVG(price) FROM real_estate_listings WHERE price > 0;
```

## 📊 Интеграция с парсером

Парсер автоматически сохраняет все объявления в эту базу данных:

1. **Создание сессии** - при каждом запуске парсинга
2. **Сохранение данных** - каждое объявление дублируется в dataBD
3. **Завершение сессии** - статистика и завершение сессии
4. **Обновление статистики** - ежедневная агрегация данных

## 🔍 Мониторинг

### Размер базы данных
```bash
ls -lh dataBD/parsed_listings.db
```

### Количество записей
```bash
sqlite3 dataBD/parsed_listings.db "SELECT COUNT(*) FROM real_estate_listings;"
```

### Последние сессии парсинга
```bash
sqlite3 dataBD/parsed_listings.db "
SELECT session_id, total_parsed, total_saved, status 
FROM parsing_sessions 
ORDER BY started_at DESC 
LIMIT 5;"
```

## 🧹 Обслуживание

### Очистка старых данных
```python
# Метод отключен для упрощенной схемы (нет полей времени)
# deleted_count = databd_manager.cleanup_old_listings(days=30)
```

### Создание резервной копии
```bash
cp dataBD/parsed_listings.db dataBD/backup_$(date +%Y%m%d).db
```

### Экспорт данных
```python
# Создать ежедневный снимок в разных форматах
from data_exporter import DataExporter
exporter = DataExporter("dataBD")
files = exporter.export_daily_snapshot()
```

## 📈 Статистика

По состоянию на последнее обновление:
- **Всего объявлений**: 1 (тестовая база)
- **Размер базы**: 20KB
- **Источники**: cian
- **Завершенных сессий**: 1
- **Схема**: Упрощенная (14 полей)

## 🚀 Возможности

- ✅ Автоматическое сохранение при парсинге
- ✅ Отслеживание сессий парсинга
- ✅ Ежедневная статистика
- ✅ Индексы для быстрого поиска
- ✅ Сохранение исходных данных
- ✅ Координаты для геоанализа
- ✅ Контактная информация продавцов
- ✅ История обновлений объявлений

## 🔮 Планы развития

- [ ] Экспорт в различные форматы (CSV, Excel, JSON)
- [ ] Интеграция с изохронами Яндекс.Лавки
- [ ] Аналитические дашборды
- [ ] API для внешнего доступа
- [ ] Автоматическая архивация старых данных

---
*Создано автоматически при интеграции парсера с dataBD* 
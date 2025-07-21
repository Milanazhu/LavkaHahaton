import requests
import json
import os
from datetime import datetime, timedelta

def write_to_file(text, filename='try.md'):
    """Записывает текст в файл и выводит в консоль"""
    print(text)  # Выводим в консоль
    try:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(text + '\n')
    except Exception as e:
        print(f"Ошибка записи в файл: {e}")

def clear_output_file(filename='try.md'):
    """Очищает файл вывода"""
    try:
        # Вычисляем конкретные даты периода
        current_date = datetime.now()
        start_date = current_date - timedelta(days=30)
        period_text = f"{start_date.strftime('%d.%m.%Y')} - {current_date.strftime('%d.%m.%Y')}"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"ОТЧЕТ CIAN.RU\n")
            f.write(f"{datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n")
            f.write(f"Период: {period_text}\n\n")
    except Exception as e:
        print(f"Ошибка создания файла: {e}")

def load_seen_offers():
    """Загружает ID уже виденных объявлений"""
    try:
        if os.path.exists('seen_offers.json'):
            with open('seen_offers.json', 'r', encoding='utf-8') as f:
                return set(json.load(f))
    except:
        pass
    return set()

def save_seen_offers(offer_ids):
    """Сохраняет ID объявлений"""
    try:
        with open('seen_offers.json', 'w', encoding='utf-8') as f:
            json.dump(list(offer_ids), f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения: {e}")

def main():
    # Очищаем файл вывода
    clear_output_file()
    
    # Загружаем уже виденные объявления
    seen_offers = load_seen_offers()
    
    headers = {
        'accept': '*/*',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json',
        'origin': 'https://perm.cian.ru',
        'priority': 'u=1, i',
        'referer': 'https://perm.cian.ru/',
        'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
    }
    
    json_data = {
        'jsonQuery': {
            '_type': 'commercialrent',
            'engine_version': {
                'type': 'term',
                'value': 2,
            },
            'office_type': {
                'type': 'terms',
                'value': [
                    1,
                    3,
                ],
            },
            'is_by_commercial_owner': {
                'type': 'term',
                'value': True,
            },
            'region': {
                'type': 'terms',
                'value': [
                    4927,
                ],
            },
            'publish_period': {
                'type': 'term',
                'value': 2592000,  # 30 дней в секундах (30 * 24 * 3600)
            },
        },
    }

    try:
        response = requests.post(
            'https://api.cian.ru/commercial-search-offers/desktop/v1/offers/get-offers/',
            headers=headers,
            json=json_data,
        )
        
        response.raise_for_status()  # Проверка на ошибки HTTP
        data = response.json()
        
        # Получаем объявления из правильного поля
        offers = data.get('data', {}).get('suggestOffersSerializedList', [])
        if not offers:
            offers = data.get('data', {}).get('offersSerialized', [])
        
        total_count = data.get('data', {}).get('offerCount', 0)
        
        # Фильтруем только новые объявления
        new_offers = []
        current_offer_ids = set()
        
        for offer in offers:
            offer_id = offer.get('id')
            current_offer_ids.add(offer_id)
            if offer_id not in seen_offers:
                new_offers.append(offer)
        
        write_to_file(f"Общее количество: {total_count}")
        write_to_file(f"Новых объявлений: {len(new_offers)}")
        write_to_file(f"Уже просмотренных: {len(offers) - len(new_offers)}")
        write_to_file(f"Регион: Пермь")
        write_to_file("")
        write_to_file("="*60)
        write_to_file("")
        
        if new_offers:
            # Выводим информацию о новых объявлениях
            for i, offer in enumerate(new_offers):
                write_to_file(f"📋 ОБЪЯВЛЕНИЕ {i+1}")
                write_to_file("-" * 40)
                
                # Основная информация
                offer_id = offer.get('id', 'Не указан')
                write_to_file(f"🆔 ID: {offer_id}")
                
                # Получаем числовое значение площади для расчета цены
                area_numeric = 0
                area_text = offer.get('totalArea', 'Не указана')
                if area_text and area_text != 'Не указана':
                    try:
                        area_numeric = float(area_text)
                    except:
                        pass
                
                # Цена
                price_info = offer.get('bargainTerms', {})
                if price_info.get('price'):
                    price = price_info['price']
                    
                    if price_info.get('priceType') == 'squareMeter' and area_numeric > 0:
                        # Цена за м² - умножаем на площадь для получения общей цены
                        total_monthly_price = price * area_numeric
                        price_text = f"{total_monthly_price:,.0f} ₽/мес."
                    else:
                        # Цена уже общая за месяц
                        price_text = f"{price:,} ₽/мес."
                else:
                    price_text = offer.get('formattedShortPrice', 'Не указана')
                write_to_file(f"💰 Цена: {price_text}")
                
                # Площадь
                area = offer.get('totalArea', 'Не указана')
                if area and area != 'Не указана':
                    area = f"{area} м²"
                write_to_file(f"📏 Площадь: {area}")
                
                # Адрес
                geo = offer.get('geo', {})
                address = geo.get('userInput', 'Не указан')
                if len(address) > 80:
                    address = address[:80] + "..."
                write_to_file(f"📍 Адрес: {address}")
                
                # Тип помещения
                specialty = offer.get('specialty', {})
                types = specialty.get('types', [])
                if types:
                    types_ru = []
                    specialties = specialty.get('specialties', [])
                    for spec in specialties[:5]:  # Показываем первые 5 типов
                        types_ru.append(spec.get('rusName', ''))
                    types_text = ', '.join(filter(None, types_ru))
                    if len(specialties) > 5:
                        types_text += f" и еще {len(specialties) - 5}"
                else:
                    types_text = "Свободное назначение"
                
                if len(types_text) > 60:
                    types_text = types_text[:60] + "..."
                write_to_file(f"🏪 Назначение: {types_text}")
                
                # Ссылка
                full_url = offer.get('fullUrl', '')
                write_to_file(f"🔗 Ссылка: {full_url}")
                
                # Дополнительная информация
                floor = offer.get('floorNumber', 'Не указан')
                building = offer.get('building', {})
                floors_total = building.get('floorsCount', 'Не указано')
                write_to_file(f"🏢 Этаж: {floor}/{floors_total}")
                
                # Контакты
                phones = offer.get('phones', [])
                if phones:
                    phone_numbers = []
                    for phone in phones:
                        country_code = phone.get('countryCode', '7')
                        number = phone.get('number', '')
                        if number:
                            phone_numbers.append(f"+{country_code} {number}")
                    write_to_file(f"📞 Телефон: {', '.join(phone_numbers)}")
                
                # Время добавления
                added_time = offer.get('humanizedTimedelta', 'Не указано')
                write_to_file(f"🕒 Добавлено: {added_time}")
                
                # Описание (краткое)
                description = offer.get('description', '')
                if description:
                    # Берем первые 200 символов описания для файла
                    short_desc = description[:200] + "..." if len(description) > 200 else description
                    write_to_file(f"📝 Описание: {short_desc}")
                
                write_to_file("")
                write_to_file("="*60)
                write_to_file("")
        else:
            write_to_file("❌ Новых объявлений не найдено")
            if offers:
                write_to_file(f"Все {len(offers)} объявлений уже были показаны ранее")
        
        # Сохраняем актуальный список ID
        save_seen_offers(current_offer_ids)
        
        write_to_file(f"\nОтчет сгенерирован: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
        write_to_file("Источник: Cian.ru")
            
    except requests.RequestException as e:
        error_msg = f"Ошибка при запросе: {e}"
        write_to_file(error_msg)
    except Exception as e:
        error_msg = f"Ошибка обработки данных: {e}"
        write_to_file(error_msg)

if __name__ == "__main__":
    main()
import asyncio
import logging
import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

from config import BOT_TOKEN, MAX_MESSAGE_LENGTH, SAFE_MODE_ENABLED
from parser import parser
from dataBD_manager import databd_manager
from safe_mode import safe_mode
# Используем databd_manager как основную БД
db_manager = databd_manager

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class CianBot:
    """Телеграм бот для парсинга объявлений с Cian.ru"""
    
    def __init__(self):
        self.application = Application.builder().token(BOT_TOKEN).build()
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Настраивает обработчики команд и кнопок"""
        # Команды
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CommandHandler("safety", self.safety_command))
        
        # Callback кнопки
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user
        logger.info(f"Пользователь {user.id} ({user.first_name}) запустил бота")
        
        # Сохраняем информацию о пользователе в БД
        try:
            db_manager.save_user(
                user_id=str(user.id),
                first_name=user.first_name,
                last_name=user.last_name,
                username=user.username
            )
        except Exception as e:
            logger.error(f"Ошибка сохранения пользователя {user.id}: {e}")
        
        welcome_text = (
            f"🏢 *Добро пожаловать, {user.first_name}!*\n\n"
            "Этот бот поможет вам отслеживать новые объявления коммерческой аренды "
            "офисных помещений в Перми с сайта Cian.ru\n\n"
            "📊 *Что умеет бот:*\n"
            "• Парсит объявления за последний месяц\n"
            "• Отслеживает только новые объявления\n"
            "• Создает подробные Excel отчеты\n"
            "• Показывает статистику и аналитику\n\n"
            "🚀 *Выберите действие:*"
        )
        
        keyboard = self._get_main_keyboard()
        
        await update.message.reply_text(
            welcome_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard
        )
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /help"""
        help_text = (
            "🆘 *Помощь по использованию бота*\n\n"
            "🚀 *Start* - Запускает полный парсинг объявлений с Cian.ru\n"
            "• Ищет все объявления за последний месяц\n"
            "• Создает Excel отчет с тремя листами\n"
            "• Сохраняет информацию о просмотренных объявлениях\n\n"
            "🔄 *Update* - Проверяет новые объявления\n"
            "• Показывает только объявления, которых не было при последнем запуске Start\n"
            "• Создает отчет только при наличии новых объявлений\n"
            "• Идеально для регулярной проверки\n\n"
            "📊 *Excel отчет содержит:*\n"
            "• Лист 'Статистика' - общая информация\n"
            "• Лист 'Объявления' - подробная таблица\n"
            "• Лист 'Сводка по ценам' - аналитика\n\n"
            "❓ *Вопросы?* Напишите /start для возврата в главное меню"
        )
        
        keyboard = self._get_main_keyboard()
        
        await update.message.reply_text(
            help_text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=keyboard
        )
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /stats - показывает статистику пользователя"""
        user = update.effective_user
        user_id = str(user.id)
        
        try:
            # Получаем статистику пользователя
            user_stats = db_manager.get_user_stats(user_id)
            db_stats = db_manager.get_database_stats()
            
            stats_text = (
                f"📊 *Ваша статистика, {user.first_name}*\n\n"
                f"👀 Просмотрено объявлений: {user_stats.get('seen_count', 0)}\n"
                f"🕐 Первый просмотр: {user_stats.get('first_seen', 'Нет данных')}\n"
                f"🕑 Последний просмотр: {user_stats.get('last_seen', 'Нет данных')}\n\n"
                f"🗂️ *Общая статистика базы данных:*\n"
                f"🏢 Всего объявлений: {db_stats.get('total_offers', 0)}\n"
                f"👥 Всего пользователей: {db_stats.get('total_users', 0)}\n"
                f"💰 Средняя цена: {db_stats.get('avg_price', 0):,.0f} ₽/мес\n"
                f"📅 Последнее обновление: {db_stats.get('last_update', 'Нет данных')}\n\n"
                f"💡 *Доступные команды:*\n"
                f"/start - Главное меню\n"
                f"/help - Помощь\n"
                f"/stats - Эта статистика\n"
                f"/safety - Безопасный режим"
            )
            
            keyboard = self._get_main_keyboard()
            
            await update.message.reply_text(
                stats_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard
            )
            
        except Exception as e:
            logger.error(f"Ошибка получения статистики для {user_id}: {e}")
            await update.message.reply_text(
                "❌ *Ошибка получения статистики*\n\n"
                "Не удалось загрузить данные из базы. Попробуйте позже.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self._get_main_keyboard()
            )
    
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на кнопки"""
        query = update.callback_query
        user_id = str(query.from_user.id)
        action = query.data
        
        await query.answer()  # Убираем "часики" с кнопки
        
        logger.info(f"Пользователь {user_id} нажал кнопку: {action}")
        
        if action == "start_parsing":
            await self._handle_start_parsing(query, user_id)
        elif action == "update_parsing":
            await self._handle_update_parsing(query, user_id)
        elif action == "help":
            await self.help_command(update, context)
        else:
            await query.edit_message_text("❌ Неизвестная команда")
    
    async def _handle_start_parsing(self, query, user_id: str):
        """Обработка кнопки Start - полный парсинг с проверкой безопасного режима"""
        try:
            # Проверяем безопасный режим перед запуском (только если включен)
            if SAFE_MODE_ENABLED:
                can_parse, safety_info = safe_mode.can_parse(user_id)
                
                if not can_parse:
                    # Парсинг заблокирован - показываем информацию
                    await query.edit_message_text(
                        f"🛡️ *Безопасный режим активен*\n\n"
                        f"🚫 {safety_info.get('message', 'Парсинг временно недоступен')}\n\n"
                        f"⏰ *Осталось ждать:*\n"
                        f"{safety_info.get('hours_left', 0)} ч. {safety_info.get('minutes_left', 0)} мин.\n\n"
                        f"📅 *Следующий парсинг доступен:*\n"
                        f"{safety_info.get('next_available', 'Неизвестно')}\n\n"
                        f"📊 *Ваша статистика:*\n"
                        f"• Сегодня: {safety_info.get('total_today', 0)} парсингов\n"
                        f"• Всего: {safety_info.get('total_all_time', 0)} парсингов\n\n"
                        f"💡 Используйте /safety для подробной информации",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=self._get_main_keyboard()
                    )
                    return
            
            # Показываем сообщение о начале работы
            safety_status = "✅ Разрешено" if SAFE_MODE_ENABLED else "⚠️ Отключен"
            await query.edit_message_text(
                "🚀 *Запуск парсинга объявлений...*\n\n"
                f"🛡️ Безопасный режим: {safety_status}\n"
                "⏳ Выполняется поиск объявлений на Cian.ru\n"
                "Пожалуйста, подождите...",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Выполняем парсинг (все объявления)
            offers, stats = parser.parse_offers(user_id=user_id, only_new=False)
            
            # Формируем текст результата
            result_text = self._format_results_text(stats, len(offers), "полный поиск")
            
            # Отправляем результат
            await self._send_results(query, result_text, offers)
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге для пользователя {user_id}: {e}")
            await query.edit_message_text(
                f"❌ *Ошибка при парсинге*\n\n"
                f"Произошла ошибка: {str(e)}\n\n"
                f"Попробуйте позже или обратитесь к администратору.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self._get_main_keyboard()
            )
    
    async def _handle_update_parsing(self, query, user_id: str):
        """Обработка кнопки Update - только новые объявления с проверкой безопасного режима"""
        try:
            # Проверяем безопасный режим перед запуском (только если включен)
            if SAFE_MODE_ENABLED:
                can_parse, safety_info = safe_mode.can_parse(user_id)
                
                if not can_parse:
                    # Парсинг заблокирован - показываем информацию
                    await query.edit_message_text(
                        f"🛡️ *Безопасный режим активен*\n\n"
                        f"🚫 {safety_info.get('message', 'Парсинг временно недоступен')}\n\n"
                        f"⏰ *Осталось ждать:*\n"
                        f"{safety_info.get('hours_left', 0)} ч. {safety_info.get('minutes_left', 0)} мин.\n\n"
                        f"📅 *Следующий парсинг доступен:*\n"
                        f"{safety_info.get('next_available', 'Неизвестно')}\n\n"
                        f"💡 Используйте /safety для подробной информации",
                        parse_mode=ParseMode.MARKDOWN,
                        reply_markup=self._get_main_keyboard()
                    )
                    return
            
            # Показываем сообщение о начале работы
            safety_status = "✅ Разрешено" if SAFE_MODE_ENABLED else "⚠️ Отключен"
            await query.edit_message_text(
                "🔄 *Проверка новых объявлений...*\n\n"
                f"🛡️ Безопасный режим: {safety_status}\n"
                "⏳ Ищем объявления, которых не было ранее\n"
                "Пожалуйста, подождите...",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Выполняем парсинг (только новые)
            offers, stats = parser.parse_offers(user_id=user_id, only_new=True)
            
            if offers:
                # Есть новые объявления - создаем отчет
                result_text = self._format_results_text(stats, len(offers), "обновление")
                await self._send_results(query, result_text, offers)
            else:
                # Новых объявлений нет
                result_text = (
                    "✅ *Обновление завершено*\n\n"
                    f"📊 Найдено объявлений: {stats['total_count']}\n"
                    f"🆕 Новых объявлений: 0\n"
                    f"👁 Уже просмотренных: {stats['seen_count']}\n\n"
                    f"🕐 Время проверки: {stats['search_time']}\n\n"
                    "💡 Все объявления уже были показаны ранее.\n"
                    "Используйте кнопку Update позже для проверки новых объявлений."
                )
                
                await query.edit_message_text(
                    result_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=self._get_main_keyboard()
                )
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении для пользователя {user_id}: {e}")
            await query.edit_message_text(
                f"❌ *Ошибка при обновлении*\n\n"
                f"Произошла ошибка: {str(e)}\n\n"
                f"Попробуйте позже или обратитесь к администратору.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self._get_main_keyboard()
            )
    
    def _format_results_text(self, stats: dict, offers_count: int, operation_type: str) -> str:
        """Форматирует текст с результатами парсинга"""
        return (
            f"✅ *{operation_type.title()} завершен*\n\n"
            f"📊 Общее количество объявлений: {stats['total_count']}\n"
            f"🆕 Показано объявлений: {offers_count}\n"
            f"👁 Уже просмотренных: {stats['seen_count']}\n\n"
            f"🕐 Время поиска: {stats['search_time']}\n"
            f"📍 Регион: Пермь\n"
            f"📅 Период: Последний месяц\n\n"
            f"📄 Excel отчет прикреплен к сообщению"
        )
    
    async def _send_results(self, query, text: str, offers: list):
        """Отправляет результаты пользователю"""
        try:
            # Отправляем текстовый отчет
            await query.edit_message_text(
                text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self._get_main_keyboard()
            )
            
            # Если есть объявления с фотографиями, отправляем превью первого
            if offers and offers[0].get('photos'):
                try:
                    first_offer = offers[0]
                    photo_url = first_offer['photos'][0]
                    caption = (
                        f"🏢 *Превью первого объявления*\n\n"
                        f"💰 {first_offer.get('price_text', 'Цена не указана')}\n"
                        f"📏 {first_offer.get('area', 'Площадь не указана')}\n"
                        f"📍 {first_offer.get('address', 'Адрес не указан')}"
                    )
                    
                    await query.message.reply_photo(
                        photo=photo_url,
                        caption=caption,
                        parse_mode=ParseMode.MARKDOWN
                    )
                except Exception as e:
                    logger.warning(f"Не удалось отправить превью фото: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки результатов: {e}")
            await query.edit_message_text(
                "✅ Парсинг завершен, но возникла ошибка при отправке файла.\n"
                "Попробуйте еще раз.",
                reply_markup=self._get_main_keyboard()
            )
    
    def _get_main_keyboard(self):
        """Создает основную клавиатуру с кнопками"""
        keyboard = [
            [
                InlineKeyboardButton("🚀 Start", callback_data="start_parsing"),
                InlineKeyboardButton("🔄 Update", callback_data="update_parsing")
            ],
            [
                InlineKeyboardButton("❓ Помощь", callback_data="help")
            ]
        ]
        return InlineKeyboardMarkup(keyboard)
    
    async def safety_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показывает статус безопасного режима"""
        user = update.effective_user
        user_id = str(user.id)
        
        try:
            # Проверяем, включен ли безопасный режим
            if not SAFE_MODE_ENABLED:
                safety_text = (
                    f"⚠️ *Безопасный режим отключен*\n\n"
                    f"👤 *Пользователь:* {user.first_name}\n"
                    f"📊 *Статус:* ✅ Парсинг всегда доступен\n\n"
                    f"💡 *Информация:*\n"
                    f"Безопасный режим отключен в настройках.\n"
                    f"Парсинг можно запускать без ограничений.\n\n"
                    f"⚙️ *Настройка:*\n"
                    f"Для включения измените `SAFE_MODE_ENABLED = True` в config.py\n\n"
                    f"💼 *Команды:*\n"
                    f"/start - Главное меню\n"
                    f"/stats - Общая статистика"
                )
                
                keyboard = self._get_main_keyboard()
                
                await update.message.reply_text(
                    safety_text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=keyboard
                )
                return
            
            # Получаем статистику безопасного режима для пользователя
            safety_stats = safe_mode.get_user_safety_stats(user_id)
            
            # Проверяем, можно ли сейчас парсить
            can_parse, status_info = safe_mode.can_parse(user_id)
            
            # Формируем сообщение
            status_emoji = "✅" if can_parse else "🚫"
            status_text = "Доступен" if can_parse else "Заблокирован"
            
            safety_text = (
                f"🛡️ *Безопасный режим парсинга*\n\n"
                f"👤 *Пользователь:* {user.first_name}\n"
                f"📊 *Статус парсинга:* {status_emoji} {status_text}\n\n"
                f"📈 *Ваша статистика:*\n"
                f"🗓️ Парсингов сегодня: {safety_stats.get('today_count', 0)}\n"
                f"📊 Всего парсингов: {safety_stats.get('total_count', 0)}\n"
                f"🕐 Последний парсинг: {safety_stats.get('last_parsing', 'Никогда')}\n\n"
                f"⏰ *Ограничения:*\n"
                f"🔒 Интервал безопасности: {safety_stats.get('safety_interval', '24 часа')}\n"
                f"⏳ Следующий доступен: {safety_stats.get('next_available', 'Сейчас')}\n\n"
            )
            
            # Добавляем специфичную информацию в зависимости от статуса
            if not can_parse:
                safety_text += (
                    f"🚫 *Причина блокировки:*\n"
                    f"{status_info.get('message', 'Неизвестная причина')}\n\n"
                    f"⏰ *Осталось ждать:*\n"
                    f"{status_info.get('hours_left', 0)} ч. {status_info.get('minutes_left', 0)} мин.\n\n"
                )
            else:
                safety_text += (
                    f"✅ *Парсинг доступен!*\n"
                    f"Вы можете запустить поиск объявлений.\n\n"
                )
            
            safety_text += (
                f"💡 *О безопасном режиме:*\n"
                f"Система ограничивает парсинг одним разом в сутки для предотвращения "
                f"блокировки со стороны сайтов и обеспечения стабильной работы.\n\n"
                f"💼 *Команды:*\n"
                f"/start - Главное меню\n"
                f"/stats - Общая статистика\n"
                f"/safety - Этот экран"
            )
            
            keyboard = self._get_main_keyboard()
            
            await update.message.reply_text(
                safety_text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=keyboard
            )
            
        except Exception as e:
            logger.error(f"Ошибка показа безопасного режима для {user_id}: {e}")
            await update.message.reply_text(
                "❌ *Ошибка безопасного режима*\n\n"
                "Не удалось загрузить данные безопасности. Попробуйте позже.\n\n"
                f"Ошибка: {str(e)}",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=self._get_main_keyboard()
            )
    
    def run(self):
        """Запускает бота"""
        logger.info("Запуск Cian Parser Bot...")
        self.application.run_polling(allowed_updates=Update.ALL_TYPES)

def main():
    """Основная функция"""
    bot = CianBot()
    bot.run()

if __name__ == "__main__":
    main() 
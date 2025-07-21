import asyncio
import logging
import os
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.constants import ParseMode

from config import BOT_TOKEN, MAX_MESSAGE_LENGTH
from parser import parser
from excel_generator import excel_generator

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
        
        # Callback кнопки
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
    
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик команды /start"""
        user = update.effective_user
        logger.info(f"Пользователь {user.id} ({user.first_name}) запустил бота")
        
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
        """Обработка кнопки Start - полный парсинг"""
        try:
            # Показываем сообщение о начале работы
            await query.edit_message_text(
                "🚀 *Запуск парсинга объявлений...*\n\n"
                "⏳ Выполняется поиск объявлений на Cian.ru\n"
                "Пожалуйста, подождите...",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Выполняем парсинг (все объявления)
            offers, stats = parser.parse_offers(user_id=user_id, only_new=False)
            
            # Создаем Excel отчет
            excel_path = excel_generator.generate_report(offers, stats, user_id)
            
            # Формируем текст результата
            result_text = self._format_results_text(stats, len(offers), "полный поиск")
            
            # Отправляем результат
            await self._send_results(query, result_text, excel_path, offers)
            
            # Очищаем старые отчеты
            excel_generator.cleanup_old_reports(user_id)
            
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
        """Обработка кнопки Update - только новые объявления"""
        try:
            # Показываем сообщение о начале работы
            await query.edit_message_text(
                "🔄 *Проверка новых объявлений...*\n\n"
                "⏳ Ищем объявления, которых не было ранее\n"
                "Пожалуйста, подождите...",
                parse_mode=ParseMode.MARKDOWN
            )
            
            # Выполняем парсинг (только новые)
            offers, stats = parser.parse_offers(user_id=user_id, only_new=True)
            
            if offers:
                # Есть новые объявления - создаем отчет
                excel_path = excel_generator.generate_report(offers, stats, user_id)
                result_text = self._format_results_text(stats, len(offers), "обновление")
                await self._send_results(query, result_text, excel_path, offers)
                excel_generator.cleanup_old_reports(user_id)
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
    
    async def _send_results(self, query, text: str, excel_path: str, offers: list):
        """Отправляет результаты пользователю"""
        try:
            # Отправляем файл с текстом
            with open(excel_path, 'rb') as file:
                await query.message.reply_document(
                    document=file,
                    caption=text,
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=self._get_main_keyboard()
                )
            
            # Удаляем сообщение "Выполняется поиск..."
            await query.delete_message()
            
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
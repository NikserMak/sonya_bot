import telebot
import pandas as pd
from typing import Dict, List
from telebot import types
from datetime import datetime, timedelta
import random
import time
import traceback
import numpy
import schedule
import threading
from typing import Dict, List

from config import TOKEN, ADMIN_IDS, POLL_TIME, FACT_TIME
from database import Database
from facts import SLEEP_TIPS, SLEEP_FACTS
from questions import SURVEY_QUESTIONS


# Инициализация бота и базы данных
bot = telebot.TeleBot(TOKEN)
db = Database('sleep_bot.db')

# Состояния пользователей для регистрации и опросов
user_states = {}

# Клавиатуры
def get_main_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    """Получить основную клавиатуру"""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add('Моя статистика', 'Мои достижения')
    if user_id in ADMIN_IDS:
        keyboard.add('Админ-панель')
    return keyboard

def get_lifestyle_keyboard() -> types.ReplyKeyboardMarkup:
    """Получить клавиатуру для выбора образа жизни"""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    keyboard.add(
        types.KeyboardButton('Активный (регулярные тренировки, подвижная работа)'),
        types.KeyboardButton('Малоподвижный (редкие тренировки, сидячая работа)'),
        types.KeyboardButton('Сидячий (нет тренировок, сидячая работа)')
    )
    return keyboard

def get_gender_keyboard() -> types.ReplyKeyboardMarkup:
    """Получить клавиатуру для выбора пола"""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        types.KeyboardButton('Мужской'),
        types.KeyboardButton('Женский'),
        types.KeyboardButton('Другой')
    )
    return keyboard

def get_admin_keyboard() -> types.ReplyKeyboardMarkup:
    """Получить клавиатуру админа"""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        'Количество пользователей',
        'Добавить совет/факт',
        'Отправить сообщение всем',
        'Тестовый запуск',
        'Назад'
    )
    return keyboard

def get_feedback_keyboard() -> types.InlineKeyboardMarkup:
    """Получить инлайн-клавиатуру для отзыва о рекомендации"""
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton('👍 Помогло', callback_data='feedback_yes'),
        types.InlineKeyboardButton('👎 Не помогло', callback_data='feedback_no')
    )
    return keyboard

def get_test_run_keyboard() -> types.ReplyKeyboardMarkup:
    """Получить клавиатуру для тестового запуска"""
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        'Отправить совет',
        'Отправить опрос',
        'Анализ и рекомендации',
        'Назад'
    )
    return keyboard

# Обработчики команд
@bot.message_handler(commands=['start'])
def handle_start(message: types.Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    username = message.from_user.first_name
    
    # Проверяем, зарегистрирован ли пользователь
    user = db.get_user(user_id)
    
    if user:
        # Пользователь уже зарегистрирован
        bot.send_message(
            user_id, 
            f"С возвращением, {username}! 😊\nЯ СОНЯ - ваш помощник для улучшения качества сна.\n\n"
            "Вы можете посмотреть свою статистику или дождаться утреннего опроса.",
            reply_markup=get_main_keyboard(user_id)
        )
        db.update_user_activity(user_id)
    else:
        # Начинаем процесс регистрации
        bot.send_message(
            user_id, 
            f"Привет, {username}! 😊\nЯ СОНЯ - твой помощник для улучшения качества сна.\n\n"
            "Для начала давай познакомимся! Это займет всего пару минут.\n\n"
            "Сколько тебе лет? (Введи число)"
        )
        user_states[user_id] = {'state': 'registration', 'step': 'age'}

@bot.message_handler(commands=['help'])
def handle_help(message: types.Message):
    """Обработчик команды /help"""
    help_text = (
        "Я СОНЯ - бот для улучшения качества сна. Вот что я умею:\n\n"
        "📊 Ежедневный опрос о качестве сна\n"
        "💡 Полезные советы и факты о сне\n"
        "📈 Персонализированные рекомендации\n"
        "🏆 Достижения за регулярное использование\n\n"
        "Основные команды:\n"
        "/start - начать работу с ботом\n"
        "/help - показать эту справку\n\n"
        "Просто нажимай на кнопки в меню, чтобы взаимодействовать со мной!"
    )
    bot.send_message(message.from_user.id, help_text)

# Обработчики сообщений
@bot.message_handler(func=lambda message: message.text == 'Назад')
def handle_back(message: types.Message):
    """Обработчик кнопки 'Назад'"""
    user_id = message.from_user.id
    if user_id in ADMIN_IDS:
        bot.send_message(
            user_id, 
            "Возвращаемся в главное меню.", 
            reply_markup=get_main_keyboard(user_id)
        )
    else:
        bot.send_message(
            user_id, 
            "Возвращаемся в главное меню.", 
            reply_markup=get_main_keyboard(user_id)
        )

@bot.message_handler(func=lambda message: message.text == 'Моя статистика')
def handle_stats(message: types.Message):
    """Обработчик кнопки 'Моя статистика'"""
    user_id = message.from_user.id
    user = db.get_user(user_id)
    
    if not user:
        bot.send_message(user_id, "Сначала нужно зарегистрироваться. Напишите /start")
        return
    
    stats = db.get_user_stats(user_id)
    
    if not stats or 'sleep_stats' not in stats:
        bot.send_message(user_id, "У вас пока недостаточно данных для статистики. Пожалуйста, заполните несколько опросов.")
        return
    
    sleep_stats = stats['sleep_stats']
    last_week = stats.get('last_week', [])
    
    # Формируем текст статистики
    stats_text = (
        f"📊 Ваша статистика сна:\n\n"
        f"Средняя продолжительность сна: {sleep_stats['avg_sleep_duration']} часов\n"
        f"Среднее качество сна: {sleep_stats['avg_sleep_quality']}/10\n"
        f"Среднее количество пробуждений: {sleep_stats['avg_awakenings']}\n"
        f"Всего заполненных опросов: {sleep_stats['total_surveys']}\n\n"
        f"Последние 7 дней:\n"
    )
    
    for day in last_week:
        stats_text += f"{day['date']}: {day['duration']} ч, качество {day['quality']}/10\n"
    
    bot.send_message(user_id, stats_text)

@bot.message_handler(func=lambda message: message.text == 'Мои достижения')
def handle_achievements(message: types.Message):
    """Обработчик кнопки 'Мои достижения'"""
    user_id = message.from_user.id
    user = db.get_user(user_id)
    
    if not user:
        bot.send_message(user_id, "Сначала нужно зарегистрироваться. Напишите /start")
        return
    
    achievements = db.get_user_achievements(user_id)
    
    if not achievements:
        bot.send_message(user_id, "У вас пока нет достижений. Продолжайте заполнять опросы, чтобы их получить!")
        return
    
    achievements_text = "🏆 Ваши достижения:\n\n"
    for ach in achievements:
        achievements_text += f"{ach['type']} - {ach['date']}\n"
    
    bot.send_message(user_id, achievements_text)

@bot.message_handler(func=lambda message: message.text == 'Админ-панель' and message.from_user.id in ADMIN_IDS)
def handle_admin_panel(message: types.Message):
    """Обработчик кнопки 'Админ-панель'"""
    user_id = message.from_user.id
    bot.send_message(
        user_id, 
        "Добро пожаловать в админ-панель! Что вы хотите сделать?", 
        reply_markup=get_admin_keyboard()
    )

@bot.message_handler(func=lambda message: message.text == 'Количество пользователей' and message.from_user.id in ADMIN_IDS)
def handle_user_count(message: types.Message):
    """Обработчик кнопки 'Количество пользователей'"""
    user_id = message.from_user.id
    count = db.get_user_count()
    bot.send_message(user_id, f"Всего зарегистрированных пользователей: {count}")

@bot.message_handler(func=lambda message: message.text == 'Добавить совет/факт' and message.from_user.id in ADMIN_IDS)
def handle_add_fact(message: types.Message):
    """Обработчик кнопки 'Добавить совет/факт'"""
    user_id = message.from_user.id
    msg = bot.send_message(
        user_id, 
        "Введите текст совета или факта, который хотите добавить.\n\n"
        "В начале сообщения укажите тип:\n"
        "СОВЕТ: для советов по улучшению сна\n"
        "ФАКТ: для интересных фактов о сне"
    )
    bot.register_next_step_handler(msg, process_fact_type)

def process_fact_type(message: types.Message):
    """Обработчик типа факта/совета"""
    user_id = message.from_user.id
    text = message.text
    
    if text.startswith('СОВЕТ:'):
        fact_text = text.replace('СОВЕТ:', '').strip()
        if db.add_fact(fact_text, "tip"):
            bot.send_message(user_id, "Совет успешно добавлен!", reply_markup=get_admin_keyboard())
        else:
            bot.send_message(user_id, "Ошибка при добавлении совета.", reply_markup=get_admin_keyboard())
    elif text.startswith('ФАКТ:'):
        fact_text = text.replace('ФАКТ:', '').strip()
        if db.add_fact(fact_text, "fact"):
            bot.send_message(user_id, "Факт успешно добавлен!", reply_markup=get_admin_keyboard())
        else:
            bot.send_message(user_id, "Ошибка при добавлении факта.", reply_markup=get_admin_keyboard())
    else:
        msg = bot.send_message(
            user_id, 
            "Пожалуйста, укажите тип (СОВЕТ: или ФАКТ:) в начале сообщения. Попробуйте еще раз."
        )
        bot.register_next_step_handler(msg, process_fact_type)

@bot.message_handler(func=lambda message: message.text == 'Отправить сообщение всем' and message.from_user.id in ADMIN_IDS)
def handle_send_to_all(message: types.Message):
    """Обработчик кнопки 'Отправить сообщение всем'"""
    user_id = message.from_user.id
    msg = bot.send_message(
        user_id, 
        "Введите сообщение, которое хотите отправить всем пользователям:"
    )
    bot.register_next_step_handler(msg, process_message_to_all)

def process_message_to_all(message: types.Message):
    """Обработчик сообщения для всех пользователей"""
    user_id = message.from_user.id
    text = message.text
    
    users = db.get_all_users()
    success = 0
    failures = 0
    
    bot.send_message(user_id, f"Начинаю рассылку для {len(users)} пользователей...")
    
    for user in users:
        try:
            bot.send_message(user, f"📢 Сообщение от администратора:\n\n{text}")
            success += 1
            time.sleep(0.1)  # Чтобы не превысить лимиты Telegram
        except Exception as e:
            failures += 1
    
    bot.send_message(
        user_id, 
        f"Рассылка завершена!\n\nУспешно: {success}\nНе удалось: {failures}",
        reply_markup=get_admin_keyboard()
    )

@bot.message_handler(func=lambda message: message.text == 'Тестовый запуск' and message.from_user.id in ADMIN_IDS)
def handle_test_run(message: types.Message):
    """Обработчик кнопки 'Тестовый запуск'"""
    user_id = message.from_user.id
    bot.send_message(
        user_id, 
        "Выберите действие для тестового запуска:",
        reply_markup=get_test_run_keyboard()
    )

@bot.message_handler(func=lambda message: message.text == 'Отправить совет' and message.from_user.id in ADMIN_IDS)
def handle_send_test_tip(message: types.Message):
    """Обработчик кнопки 'Отправить совет'"""
    user_id = message.from_user.id
    send_daily_fact(user_id, test_mode=True)
    bot.send_message(
        user_id, 
        "Тестовый совет отправлен!",
        reply_markup=get_admin_keyboard()
    )

@bot.message_handler(func=lambda message: message.text == 'Отправить опрос' and message.from_user.id in ADMIN_IDS)
def handle_send_test_survey(message: types.Message):
    """Обработчик кнопки 'Отправить опрос'"""
    user_id = message.from_user.id
    send_daily_survey(user_id, test_mode=True)
    bot.send_message(
        user_id, 
        "Тестовый опрос отправлен!",
        reply_markup=get_admin_keyboard()
    )

def create_test_data(user_id: int):
    """Создать тестовые данные для анализа"""
    today = datetime.now()
    for i in range(7):  # Создаем данные за 7 дней
        date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
        db.save_survey(
            user_id=user_id,
            bedtime=f"{random.randint(22, 23)}:{random.randint(0, 59):02d}",
            wakeup_time=f"{random.randint(6, 8)}:{random.randint(0, 59):02d}",
            sleep_duration=random.uniform(5.5, 9.0),
            awakenings=random.randint(0, 3),
            sleep_quality=random.randint(4, 9),
            mood_morning=random.randint(4, 10),
            stress_level=random.randint(3, 8),
            exercise=random.randint(10, 60),
            caffeine=random.randint(0, 3),
            alcohol=random.randint(0, 2),
            screen_time=random.randint(30, 120),
            notes="Тестовые данные"
        )

@bot.message_handler(func=lambda message: message.text == 'Анализ и рекомендации' and message.from_user.id in ADMIN_IDS)
def handle_test_analysis(message: types.Message):
    """Обработчик кнопки 'Анализ и рекомендации'"""
    user_id = message.from_user.id
    
    try:
        # Получаем данные для анализа
        data = db.get_survey_data_for_analysis(user_id)
        
        # Проверяем, есть ли данные
        if data is None or (hasattr(data, 'empty') and data.empty):
            bot.send_message(user_id, "Нет данных для анализа. Создаю тестовые данные...")
            create_test_data(user_id)
            data = db.get_survey_data_for_analysis(user_id)
        
        # Проверяем, достаточно ли данных
        if len(data) < 7:
            bot.send_message(user_id, "Для анализа нужно как минимум 7 дней данных. Создаю дополнительные тестовые данные...")
            days_needed = 7 - len(data)
            for _ in range(days_needed):
                create_test_data(user_id)
            data = db.get_survey_data_for_analysis(user_id)
        
        # Преобразуем данные для анализа
        df = pd.DataFrame(data)
        
        # Проверяем наличие необходимых столбцов
        required_columns = ['sleep_duration', 'sleep_quality']
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Отсутствует необходимый столбец данных: {col}")
        
        # Получаем пользователя
        user = db.get_user(user_id)
        if not user:
            raise ValueError("Пользователь не найден")
        
        # Генерируем рекомендации
        recommendations = []
        
        # 1. Основные метрики
        avg_sleep = df['sleep_duration'].mean()
        avg_quality = df['sleep_quality'].mean()
        
        recommendations.append(f"🔍 Тестовый анализ (на основе {len(data)} дней данных):")
        recommendations.append(f"Средняя продолжительность сна: {avg_sleep:.1f} часов")
        recommendations.append(f"Среднее качество сна: {avg_quality:.1f}/10")
        
        # 2. Рекомендации по продолжительности
        ideal_sleep = calculate_ideal_sleep(user['age'])
        if avg_sleep < ideal_sleep - 1:
            recommendations.append(f"⚠️ Рекомендую увеличить продолжительность сна на {ideal_sleep - avg_sleep:.1f} часов")
        elif avg_sleep > ideal_sleep + 1:
            recommendations.append(f"⚠️ Вы спите больше рекомендованной нормы ({ideal_sleep} часов)")
        else:
            recommendations.append("✅ Продолжительность сна в норме")
        
        # 3. Рекомендации по качеству
        if avg_quality < 6:
            recommendations.append("⚠️ Качество сна ниже оптимального. Попробуйте:")
            recommendations.append("- Регулярное время отхода ко сну")
            recommendations.append("- Комфортные условия в спальне")
            recommendations.append("- Ограничение кофеина и экранов перед сном")
        else:
            recommendations.append("✅ Качество сна хорошее")
        
        # 4. Простые случайные рекомендации для теста
        test_tips = [
            "💡 Попробуйте выключать гаджеты за 1 час до сна",
            "💡 Теплый душ перед сном может улучшить засыпание",
            "💡 Проветривайте спальню перед сном",
            "💡 Попробуйте медитацию перед сном",
            "💡 Избегайте тяжелой пищи перед сном"
        ]
        recommendations.append(random.choice(test_tips))
        
        # Отправляем рекомендации
        bot.send_message(user_id, "\n".join(recommendations))
        bot.send_message(
            user_id, 
            "Тестовый анализ завершен!",
            reply_markup=get_admin_keyboard()
        )
    
    except Exception as e:
        error_msg = f"⚠️ Произошла ошибка при анализе: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        bot.send_message(user_id, error_msg)

# Обработчики регистрации
@bot.message_handler(func=lambda message: user_states.get(message.from_user.id, {}).get('state') == 'registration' and 
                                      user_states.get(message.from_user.id, {}).get('step') == 'age')
def handle_age(message: types.Message):
    """Обработчик возраста при регистрации"""
    user_id = message.from_user.id
    try:
        age = int(message.text)
        if age < 1 or age > 120:
            raise ValueError
        
        user_states[user_id]['age'] = age
        user_states[user_id]['step'] = 'gender'
        
        bot.send_message(
            user_id, 
            "Отлично! Теперь укажите ваш пол:",
            reply_markup=get_gender_keyboard()
        )
    except ValueError:
        bot.send_message(user_id, "Пожалуйста, введите корректный возраст (число от 1 до 120).")

@bot.message_handler(func=lambda message: user_states.get(message.from_user.id, {}).get('state') == 'registration' and 
                                      user_states.get(message.from_user.id, {}).get('step') == 'gender')
def handle_gender(message: types.Message):
    """Обработчик пола при регистрации"""
    user_id = message.from_user.id
    gender = message.text
    
    if gender not in ['Мужской', 'Женский', 'Другой']:
        bot.send_message(
            user_id, 
            "Пожалуйста, выберите пол из предложенных вариантов.",
            reply_markup=get_gender_keyboard()
        )
        return
    
    user_states[user_id]['gender'] = gender
    user_states[user_id]['step'] = 'lifestyle'
    
    bot.send_message(
        user_id, 
        "Хорошо! Теперь опишите ваш образ жизни:",
        reply_markup=get_lifestyle_keyboard()
    )

@bot.message_handler(func=lambda message: user_states.get(message.from_user.id, {}).get('state') == 'registration' and 
                                      user_states.get(message.from_user.id, {}).get('step') == 'lifestyle')
def handle_lifestyle(message: types.Message):
    """Обработчик образа жизни при регистрации"""
    user_id = message.from_user.id
    lifestyle = message.text
    
    if lifestyle not in [
        'Активный (регулярные тренировки, подвижная работа)',
        'Малоподвижный (редкие тренировки, сидячая работа)',
        'Сидячий (нет тренировок, сидячая работа)'
    ]:
        bot.send_message(
            user_id, 
            "Пожалуйста, выберите вариант из предложенных.",
            reply_markup=get_lifestyle_keyboard()
        )
        return
    
    # Завершаем регистрацию
    username = message.from_user.first_name
    age = user_states[user_id]['age']
    gender = user_states[user_id]['gender']
    
    if db.register_user(user_id, username, age, gender, lifestyle):
        bot.send_message(
            user_id, 
            f"Регистрация завершена, {username}! 🎉\n\n"
            "Теперь я буду помогать тебе улучшить качество сна.\n\n"
            "Каждое утро я буду присылать тебе небольшой опрос о твоем сне, "
            "а вечером - полезный совет или интересный факт о сне.\n\n"
            "Раз в неделю я буду анализировать твои данные и давать персонализированные рекомендации!\n\n"
            "Ты можешь в любой момент посмотреть свою статистику и достижения.",
            reply_markup=get_main_keyboard(user_id)
        )
    else:
        bot.send_message(
            user_id, 
            "Произошла ошибка при регистрации. Пожалуйста, попробуйте позже."
        )
    
    del user_states[user_id]

# Обработчики опроса
def send_daily_survey(user_id: int, test_mode: bool = False):
    """Отправить ежедневный опрос"""
    user = db.get_user(user_id)
    if not user:
        return
    
    if not test_mode:
        # Проверяем, не заполнял ли пользователь уже опрос сегодня
        today = datetime.now().strftime('%Y-%m-%d')
        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM surveys WHERE user_id = ? AND date = ?', (user_id, today))
            if cursor.fetchone()[0] > 0:
                return  # Уже заполнял опрос сегодня
    
    # Начинаем опрос
    user_states[user_id] = {
        'state': 'survey',
        'step': 0,
        'answers': {}
    }
    
    first_question = SURVEY_QUESTIONS[0]
    ask_question(user_id, first_question)

def ask_question(user_id: int, question: Dict):
    """Задать вопрос из опроса"""
    if question['type'] == 'time':
        msg = bot.send_message(
            user_id, 
            question['text'] + "\n\nВведите время в формате ЧЧ:ММ (например, 23:30)"
        )
    elif question['type'] == 'int' and question['options']:
        # Создаем клавиатуру для вариантов ответа
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=5)
        keyboard.add(*[str(opt) for opt in question['options']])
        msg = bot.send_message(
            user_id, 
            question['text'],
            reply_markup=keyboard
        )
    else:
        msg = bot.send_message(
            user_id, 
            question['text'],
            reply_markup=types.ReplyKeyboardRemove()
        )
    
    bot.register_next_step_handler(msg, process_answer, question)

def process_answer(message: types.Message, question: Dict):
    """Обработать ответ на вопрос"""
    user_id = message.from_user.id
    
    if user_id not in user_states or user_states[user_id]['state'] != 'survey':
        return
    
    try:
        # Проверяем и преобразуем ответ в нужный формат
        if question['type'] == 'time':
            # Проверяем формат времени
            time_str = message.text
            datetime.strptime(time_str, '%H:%M')
            answer = time_str
        elif question['type'] == 'int':
            answer = int(message.text)
            if question['options'] and answer not in question['options']:
                raise ValueError
        elif question['type'] == 'float':
            answer = float(message.text.replace(',', '.'))
        else:  # text
            answer = message.text
        
        # Сохраняем ответ
        user_states[user_id]['answers'][question['key']] = answer
        
        # Переходим к следующему вопросу или завершаем опрос
        next_step = user_states[user_id]['step'] + 1
        if next_step < len(SURVEY_QUESTIONS):
            user_states[user_id]['step'] = next_step
            ask_question(user_id, SURVEY_QUESTIONS[next_step])
        else:
            complete_survey(user_id)
    except ValueError:
        # Неверный формат ответа
        if question['type'] == 'time':
            error_msg = "Пожалуйста, введите время в формате ЧЧ:ММ (например, 23:30)"
        elif question['type'] in ['int', 'float']:
            error_msg = "Пожалуйста, введите число"
            if question['options']:
                error_msg += f" из предложенных вариантов: {', '.join(map(str, question['options']))}"
        else:
            error_msg = "Пожалуйста, введите корректный ответ"
        
        msg = bot.send_message(user_id, error_msg)
        bot.register_next_step_handler(msg, process_answer, question)

def complete_survey(user_id: int):
    """Завершить опрос и сохранить результаты"""
    answers = user_states[user_id]['answers']
    
    # Сохраняем результаты в базу данных
    db.save_survey(
        user_id=user_id,
        bedtime=answers.get('bedtime', ''),
        wakeup_time=answers.get('wakeup_time', ''),
        sleep_duration=answers.get('sleep_duration', 0),
        awakenings=answers.get('awakenings', 0),
        sleep_quality=answers.get('sleep_quality', 0),
        mood_morning=answers.get('mood_morning', 0),
        stress_level=answers.get('stress_level', 0),
        exercise=answers.get('exercise', 0),
        caffeine=answers.get('caffeine', 0),
        alcohol=answers.get('alcohol', 0),
        screen_time=answers.get('screen_time', 0),
        notes=answers.get('notes', '')
    )
    
    del user_states[user_id]
    
    # Отправляем благодарность
    bot.send_message(
        user_id, 
        "Спасибо за заполнение опроса! 💤\n\n"
        "Эти данные помогут мне лучше понять твой сон и давать более точные рекомендации.\n\n"
        "Вечером я пришлю тебе полезный совет или интересный факт о сне!",
        reply_markup=get_main_keyboard(user_id)
    )

# Обработчики советов и фактов
def send_daily_fact(user_id: int, test_mode: bool = False):
    """Отправить ежедневный совет или факт"""
    user = db.get_user(user_id)
    if not user:
        return
    
    if not test_mode:
        # Проверяем, не отправляли ли уже сегодня
        today = datetime.now().strftime('%Y-%m-%d')
        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT COUNT(*) FROM recommendations 
            WHERE user_id = ? AND date = ? AND recommendation_text LIKE 'Совет:%' OR recommendation_text LIKE 'Факт:%'
            ''', (user_id, today))
            if cursor.fetchone()[0] > 0:
                return  # Уже отправляли сегодня
    
    # Выбираем случайный совет или факт
    if random.random() < 0.7:  # 70% chance for a tip
        fact = random.choice(SLEEP_TIPS)
        fact_type = "Совет"
    else:
        fact = random.choice(SLEEP_FACTS)
        fact_type = "Факт"
    
    # Сохраняем в базу как рекомендацию
    db.save_recommendation(user_id, f"{fact_type}: {fact}")
    
    # Отправляем пользователю
    if fact_type == "Совет":
        emoji = "💡"
        header = "Совет дня для улучшения сна:"
    else:
        emoji = "🌙"
        header = "Интересный факт о сне:"
    
    bot.send_message(
        user_id, 
        f"{emoji} {header}\n\n{fact}"
    )

# Обработчики анализа и рекомендаций
def analyze_and_recommend(user_id: int, test_mode: bool = False):
    """Провести расширенный анализ данных и отправить персонализированные рекомендации"""
    user = db.get_user(user_id)
    if not user:
        return
    
    if not test_mode:
        last_week = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        with db._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT COUNT(*) FROM recommendations 
            WHERE user_id = ? AND date >= ? AND recommendation_text NOT LIKE 'Совет:%' AND recommendation_text NOT LIKE 'Факт:%'
            ''', (user_id, last_week))
            if cursor.fetchone()[0] > 0:
                return
    
    data = db.get_survey_data_for_analysis(user_id)
    
    if len(data) < 7:  # Минимум неделя данных
        if not test_mode:
            bot.send_message(
                user_id, 
                "Для точного анализа мне нужно больше данных о твоем сне. "
                "Пожалуйста, заполни опросы еще несколько дней."
            )
        return
    
    # Преобразуем данные для анализа
    data = preprocess_data(data)
    
    # Основные метрики
    recommendations = analyze_basic_metrics(data, user)
    
    # Углубленный корреляционный анализ
    corr_recommendations = advanced_correlation_analysis(data, user)
    recommendations.extend(corr_recommendations)
    
    # Анализ временных рядов
    time_series_recommendations = analyze_time_series(data, user)
    recommendations.extend(time_series_recommendations)
    
    # Кластерный анализ дней
    cluster_recommendations = cluster_analysis(data, user)
    recommendations.extend(cluster_recommendations)
    
    # Персонализированные советы на основе профиля
    profile_recommendations = generate_profile_based_recommendations(user)
    recommendations.extend(profile_recommendations)
    
    if not recommendations:
        recommendations = [
            "Пока у меня нет конкретных рекомендаций. Продолжай заполнять опросы!"
        ]
    
    # Удаляем дубликаты и сохраняем
    unique_recommendations = list(dict.fromkeys(recommendations))
    
    # В тестовом режиме сразу отправляем рекомендации
    if test_mode:
        for rec in unique_recommendations[:5]:  # Ограничиваем 5 лучшими рекомендациями
            bot.send_message(
                user_id, 
                f"🌿 Тестовая персонализированная рекомендация:\n\n{rec}"
            )
    else:
        for rec in unique_recommendations[:5]:  # Ограничиваем 5 лучшими рекомендациями
            db.save_recommendation(user_id, rec)
            bot.send_message(
                user_id, 
                f"🌿 Персонализированная рекомендация:\n\n{rec}"
            )
    
    if not test_mode and unique_recommendations:
        time.sleep(2)
        bot.send_message(
            user_id, 
            "Через неделю я спрошу, помогли ли тебе мои рекомендации. "
            "Если хочешь, можешь уже сейчас написать отзыв или задать вопрос!",
            reply_markup=get_main_keyboard(user_id)
        )

def preprocess_data(data: pd.DataFrame) -> pd.DataFrame:
    """Предварительная обработка данных"""
    # Преобразуем время отхода ко сну в числовой формат (часы с десятичной частью)
    data['bedtime_num'] = data['bedtime'].apply(
        lambda x: float(x.split(':')[0]) + float(x.split(':')[1])/60 if pd.notnull(x) else None
    )
    
    # Аналогично для времени подъема
    data['wakeup_num'] = data['wakeup_time'].apply(
        lambda x: float(x.split(':')[0]) + float(x.split(':')[1])/60 if pd.notnull(x) else None
    )
    
    # Рассчитываем регулярность сна
    data['sleep_regularity'] = data['bedtime_num'].rolling(window=3).std().fillna(0)
    
    # Дополнительные метрики
    data['sleep_efficiency'] = data['sleep_duration'] / (data['wakeup_num'] - data['bedtime_num'] + 24*(data['wakeup_num'] < data['bedtime_num']))
    data['weekday'] = pd.to_datetime(data['date']).dt.dayofweek
    data['is_weekend'] = data['weekday'].isin([5, 6]).astype(int)
    
    return data

def analyze_basic_metrics(data: pd.DataFrame, user: Dict) -> List[str]:
    """Анализ основных метрик сна"""
    recommendations = []
    
    # Средние показатели
    avg_sleep = data['sleep_duration'].mean()
    avg_quality = data['sleep_quality'].mean()
    avg_awakenings = data['awakenings'].mean()
    avg_efficiency = data['sleep_efficiency'].mean()
    
    # Рекомендации по продолжительности
    age = user['age']
    ideal_sleep = calculate_ideal_sleep(age)
    
    if avg_sleep < ideal_sleep - 1:
        recommendations.append(
            f"Для твоего возраста ({age} лет) рекомендованная продолжительность сна {ideal_sleep}-{ideal_sleep+1} часов. "
            f"Ты спишь в среднем {avg_sleep:.1f} часов. Попробуй увеличить продолжительность сна на {ideal_sleep - avg_sleep:.1f} часов."
        )
    elif avg_sleep > ideal_sleep + 1:
        recommendations.append(
            f"Ты спишь больше ({avg_sleep:.1f} часов) рекомендованной для твоего возраста ({age} лет) нормы ({ideal_sleep} часов). "
            "Избыток сна может снижать продуктивность. Попробуй сократить время сна на 30 минут."
        )
    
    # Рекомендации по качеству
    if avg_quality < 6:
        recommendations.append(
            f"Твое среднее качество сна ({avg_quality:.1f}/10) ниже оптимального. "
            "Рассмотри возможность улучшения гигиены сна: регулярное время отхода ко сну, "
            "комфортные условия в спальне, ограничение кофеина и экранов перед сном."
        )
    
    # Эффективность сна
    if avg_efficiency < 0.85:
        recommendations.append(
            f"Твоя эффективность сна ({avg_efficiency:.1%}) ниже оптимальной (85%+). "
            "Это означает, что ты проводишь много времени в постели без сна. "
            "Попробуй ложиться только когда действительно хочешь спать."
        )
    
    return recommendations

def calculate_ideal_sleep(age: int) -> int:
    """Рассчитать идеальную продолжительность сна по возрасту"""
    if age < 18: return 9
    elif 18 <= age < 25: return 8
    elif 25 <= age < 45: return 7.5
    elif 45 <= age < 65: return 7
    else: return 7.5  # Пожилым часто нужно немного больше сна

def advanced_correlation_analysis(data: pd.DataFrame, user: Dict) -> List[str]:
    """Расширенный корреляционный анализ"""
    recommendations = []
    
    # Рассчитываем корреляции с качеством сна
    numeric_cols = [
        'stress_level', 'exercise', 'caffeine', 'alcohol', 
        'screen_time', 'bedtime_num', 'awakenings', 'mood_morning'
    ]
    
    try:
        corr_matrix = data[numeric_cols + ['sleep_quality']].corr()
        sleep_quality_corr = corr_matrix['sleep_quality'].drop('sleep_quality').abs().sort_values(ascending=False)
        
        # Анализ топ-3 факторов
        for factor in sleep_quality_corr.index[:3]:
            corr_value = corr_matrix.loc['sleep_quality', factor]
            abs_corr = abs(corr_value)
            
            if abs_corr > 0.4:  # Значимая корреляция
                if factor == 'stress_level' and corr_value < -0.4:
                    recommendations.append(
                        "Выявлена сильная отрицательная связь между уровнем стресса и качеством сна (r={:.2f}). ".format(corr_value) +
                        "Техники управления стрессом могут значительно улучшить твой сон:\n"
                        "- Вечерняя медитация или дыхательные упражнения\n"
                        "- Ведение 'списка тревог' перед сном\n"
                        "- Теплый душ или ванна за час до сна"
                    )
                
                elif factor == 'exercise' and corr_value > 0.4:
                    rec = "Физическая активность положительно влияет на твой сон (r={:.2f}). ".format(corr_value)
                    if data['exercise'].mean() < 30:
                        rec += "Ты занимаешься в среднем всего {:.1f} минут в день. ".format(data['exercise'].mean())
                        rec += "Попробуй увеличить активность до 30-40 минут, особенно аэробные упражнения."
                    else:
                        rec += "Отлично! Продолжай в том же духе, но избегай интенсивных тренировок за 3 часа до сна."
                    recommendations.append(rec)
                
                elif factor == 'screen_time' and corr_value < -0.3:
                    avg_screen_time = data['screen_time'].mean()
                    recommendations.append(
                        "Время перед экранами перед сном отрицательно влияет на качество твоего сна (r={:.2f}). ".format(corr_value) +
                        f"Ты проводишь в среднем {avg_screen_time:.1f} минут с гаджетами перед сном. Попробуй:\n"
                        "- Установить 'ночной режим' на устройствах\n"
                        "- Использовать приложения, ограничивающие синий свет\n"
                        "- Читать бумажные книги вместо электронных"
                    )
                
                elif factor == 'bedtime_num' and abs_corr > 0.3:
                    if corr_value > 0:  # Поздний отход ко сну = хуже качество
                        avg_bedtime = data['bedtime_num'].mean()
                        ideal_bedtime = 22.5 if user['age'] >= 18 else 21.5
                        if avg_bedtime > ideal_bedtime:
                            recommendations.append(
                                "Более поздний отход ко сну связан с ухудшением качества твоего сна (r={:.2f}). ".format(corr_value) +
                                f"Ты обычно ложишься в {int(avg_bedtime)}:{int((avg_bedtime%1)*60):02d}. " +
                                f"Попробуй постепенно смещать время отхода ко сну к {int(ideal_bedtime)}:{int((ideal_bedtime%1)*60):02d}."
                            )
    except Exception as e:
        print(f"Ошибка корреляционного анализа: {e}")
    
    return recommendations

def analyze_time_series(data: pd.DataFrame, user: Dict) -> List[str]:
    """Анализ временных рядов и паттернов"""
    recommendations = []
    
    try:
        # Анализ различий будни/выходные
        weekend_data = data[data['is_weekend'] == 1]
        weekday_data = data[data['is_weekend'] == 0]
        
        if len(weekend_data) > 2 and len(weekday_data) > 5:
            weekend_sleep = weekend_data['sleep_duration'].mean()
            weekday_sleep = weekday_data['sleep_duration'].mean()
            
            if abs(weekend_sleep - weekday_sleep) > 1.5:
                recommendations.append(
                    "Я заметил значительную разницу в продолжительности твоего сна в выходные ({:.1f} ч) ".format(weekend_sleep) +
                    "и будни ({:.1f} ч). ".format(weekday_sleep) +
                    "Такие колебания могут вызывать 'социальный джетлаг'. " +
                    "Попробуй сократить разницу до 1 часа, вставая в выходные не более чем на 1 час позже."
                )
        
        # Анализ тренда качества сна
        data['date_dt'] = pd.to_datetime(data['date'])
        data.set_index('date_dt', inplace=True)
        weekly_avg = data['sleep_quality'].resample('W').mean()
        
        if len(weekly_avg) > 2:
            trend = (weekly_avg.iloc[-1] - weekly_avg.iloc[0]) / len(weekly_avg)
            
            if trend < -0.3:
                recommendations.append(
                    "За последние недели я заметил ухудшение качества твоего сна. " +
                    "Это может быть связано с повышенным стрессом, изменением распорядка дня или другими факторами. " +
                    "Давай обсудим, что изменилось в твоей жизни за это время?"
                )
            elif trend > 0.3:
                recommendations.append(
                    "Отличные новости! Качество твоего сна постепенно улучшается. " +
                    "Продолжай практиковать хорошие привычки сна, которые ты выработал."
                )
    except Exception as e:
        print(f"Ошибка анализа временных рядов: {e}")
    
    return recommendations

def cluster_analysis(data: pd.DataFrame, user: Dict) -> List[str]:
    """Кластерный анализ дней по характеристикам сна"""
    recommendations = []
    
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler
        
        # Подготовка данных
        cluster_data = data[['sleep_duration', 'sleep_quality', 'awakenings', 'stress_level']].dropna()
        if len(cluster_data) < 10:
            return recommendations
        
        # Масштабирование
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(cluster_data)
        
        # Кластеризация
        kmeans = KMeans(n_clusters=2, random_state=42)
        clusters = kmeans.fit_predict(scaled_data)
        cluster_data['cluster'] = clusters
        
        # Анализ кластеров
        cluster_stats = cluster_data.groupby('cluster').mean()
        
        # Находим "хороший" и "плохой" кластеры
        good_cluster = cluster_stats['sleep_quality'].idxmax()
        bad_cluster_stats = cluster_stats.loc[1 - good_cluster]
        good_cluster_stats = cluster_stats.loc[good_cluster]
        
        # Сравниваем факторы
        significant_diffs = []
        for col in ['stress_level', 'awakenings']:
            diff = bad_cluster_stats[col] - good_cluster_stats[col]
            if diff > 0.5 * good_cluster_stats[col]:  # Значимая разница
                significant_diffs.append((col, diff))
        
        # Формируем рекомендации
        if significant_diffs:
            rec = "Анализ твоих данных выявил два типа ночей: с хорошим и плохим сном. "
            rec += "В 'плохие' ночи наблюдаются:\n"
            
            for col, diff in significant_diffs:
                if col == 'stress_level':
                    rec += f"- Значительно более высокий уровень стресса (+{diff:.1f} балла)\n"
                elif col == 'awakenings':
                    rec += f"- Больше пробуждений (+{diff:.1f} раза)\n"
            
            rec += "\nПопробуй в дни с высоким стрессом:\n"
            rec += "- Техники релаксации перед сном\n"
            rec += "- Теплый ромашковый чай\n"
            rec += "- Более ранний отход ко сну"
            
            recommendations.append(rec)
    except Exception as e:
        print(f"Ошибка кластерного анализа: {e}")
    
    return recommendations

def generate_profile_based_recommendations(user: Dict) -> List[str]:
    """Генерация рекомендаций на основе профиля пользователя"""
    recommendations = []
    age = user['age']
    gender = user['gender']
    lifestyle = user['lifestyle']
    
    # По возрасту
    if age >= 45:
        recommendations.append(
            "В твоем возрасте мелатонин (гормон сна) вырабатывается менее активно. "
            "Попробуй:\n"
            "- Увеличить воздействие естественного света днем\n"
            "- Рассмотреть добавки мелатонина после консультации с врачом\n"
            "- Соблюдать строгий режим сна"
        )
    
    # По полу
    if gender == 'Женский':
        recommendations.append(
            "Женщины часто более чувствительны к изменениям циркадных ритмов. "
            "Попробуй:\n"
            "- Стабильный график сна даже в выходные\n"
            "- Техники релаксации при ПМС\n"
            "- Более темную и прохладную спальню"
        )
    
    # По образу жизни
    if 'сидячий' in lifestyle.lower():
        recommendations.append(
            "Твой сидячий образ жизни может влиять на качество сна. "
            "Даже небольшая активность может помочь:\n"
            "- 10-минутная прогулка после ужина\n"
            "- Растяжка перед сном\n"
            "- Использование стоячего рабочего места"
        )
    elif 'активный' in lifestyle.lower():
        recommendations.append(
            "Хотя ты ведешь активный образ жизни, обрати внимание:\n"
            "- Интенсивные тренировки за 3+ часа до сна могут мешать засыпанию\n"
            "- Восстановительные практики (йога, растяжка) вечером\n"
            "- Достаточное потребление магния и белка"
        )
    
    return recommendations

# Обработчик обратной связи
@bot.callback_query_handler(func=lambda call: call.data.startswith('feedback_'))
def handle_feedback(call: types.CallbackQuery):
    """Обработчик отзыва о рекомендации"""
    user_id = call.from_user.id
    recommendation = db.get_last_recommendation(user_id)
    
    if not recommendation:
        bot.answer_callback_query(call.id, "Не удалось найти рекомендацию для отзыва.")
        return
    
    is_helpful = call.data.endswith('_yes')
    db.update_recommendation_feedback(recommendation['id'], is_helpful)
    
    if is_helpful:
        bot.answer_callback_query(call.id, "Спасибо за отзыв! Рад, что рекомендация была полезной. 😊")
    else:
        bot.answer_callback_query(call.id, "Спасибо за честный отзыв! Учту это в будущих рекомендациях.")
    
    # Удаляем клавиатуру
    bot.edit_message_reply_markup(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        reply_markup=None
    )

# Функции планировщика
def schedule_checker():
    """Проверка запланированных задач"""
    while True:
        schedule.run_pending()
        time.sleep(60)  # Проверяем каждую минуту

def send_morning_surveys():
    """Отправить утренние опросы всем пользователям"""
    users = db.get_all_users()
    for user_id in users:
        try:
            user = db.get_user(user_id)
            if user:
                # Отправляем в установленное пользователем время или в 8 утра по умолчанию
                notification_time = user.get('notification_time', POLL_TIME)
                current_time = datetime.now().strftime('%H:%M')
                if current_time == notification_time:
                    send_daily_survey(user_id)
        except Exception as e:
            print(f"Ошибка при отправке опроса пользователю {user_id}: {e}")

def send_evening_facts():
    """Отправить вечерние советы/факты всем пользователям"""
    users = db.get_all_users()
    for user_id in users:
        try:
            send_daily_fact(user_id)
        except Exception as e:
            print(f"Ошибка при отправке совета пользователю {user_id}: {e}")

def weekly_analysis():
    """Еженедельный анализ и рекомендации"""
    users = db.get_all_users()
    for user_id in users:
        try:
            analyze_and_recommend(user_id)
        except Exception as e:
            print(f"Ошибка при анализе данных пользователя {user_id}: {e}")

def ask_feedback():
    """Спросить отзыв о рекомендациях"""
    week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    users_with_recommendations = set()
    
    # Получаем пользователей, которым отправляли рекомендации неделю назад
    with db._get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT DISTINCT user_id FROM recommendations 
        WHERE date = ? AND recommendation_text NOT LIKE 'Совет:%' AND recommendation_text NOT LIKE 'Факт:%'
        ''', (week_ago,))
        users_with_recommendations.update(row[0] for row in cursor.fetchall())
    
    # Спрашиваем отзыв
    for user_id in users_with_recommendations:
        try:
            recommendation = db.get_last_recommendation(user_id)
            if recommendation:
                bot.send_message(
                    user_id,
                    f"Неделю назад я отправил тебе эту рекомендацию:\n\n{recommendation['text']}\n\n"
                    "Помогла ли она тебе улучшить сон?",
                    reply_markup=get_feedback_keyboard()
                )
        except Exception as e:
            print(f"Ошибка при запросе отзыва у пользователя {user_id}: {e}")

# Настройка расписания
schedule.every().day.at(POLL_TIME).do(send_morning_surveys)
schedule.every().day.at(FACT_TIME).do(send_evening_facts)
schedule.every().sunday.at("12:00").do(weekly_analysis)
schedule.every().sunday.at("18:00").do(ask_feedback)

# Запуск планировщика в отдельном потоке
threading.Thread(target=schedule_checker, daemon=True).start()

# Запуск бота
if __name__ == '__main__':
    print("Бот СОНЯ запущен!")
    bot.infinity_polling()
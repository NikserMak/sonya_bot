import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
import logging

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self._initialize_db()

    def _initialize_db(self):
        """Инициализация базы данных и создание таблиц"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                
                # Таблица пользователей
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    age INTEGER,
                    age_category TEXT,
                    gender TEXT,
                    lifestyle TEXT,
                    registration_date TEXT,
                    last_active_date TEXT,
                    notification_time TEXT DEFAULT '08:00'
                )
                ''')
                
                # Таблица опросов
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS surveys (
                    survey_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    date TEXT,
                    bedtime TEXT,
                    wakeup_time TEXT,
                    sleep_duration REAL,
                    awakenings INTEGER,
                    sleep_quality INTEGER,
                    mood_morning INTEGER,
                    stress_level INTEGER,
                    exercise INTEGER,
                    caffeine INTEGER,
                    alcohol INTEGER,
                    screen_time INTEGER,
                    notes TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
                ''')
                
                # Таблица рекомендаций
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS recommendations (
                    recommendation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    date TEXT,
                    recommendation_text TEXT,
                    is_helpful INTEGER DEFAULT NULL,
                    feedback_date TEXT DEFAULT NULL,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
                ''')
                
                # Таблица достижений
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS achievements (
                    achievement_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    achievement_type TEXT,
                    achievement_date TEXT,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
                ''')
                
                conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при инициализации базы данных: {e}")
            raise

    def _get_connection(self):
        """Получить соединение с базой данных"""
        return sqlite3.connect(self.db_name)

    def register_user(
        self,
        user_id: int,
        username: str,
        age: int,
        gender: str,
        lifestyle: str
    ) -> bool:
        """Регистрация нового пользователя"""
        try:
            age_category = self._determine_age_category(age)
            registration_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO users (user_id, username, age, age_category, gender, lifestyle, registration_date, last_active_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, username, age, age_category, gender, lifestyle, registration_date, registration_date))
                conn.commit()
            return True
        except sqlite3.IntegrityError:
            logger.warning(f"Пользователь {user_id} уже зарегистрирован")
            return False
        except Exception as e:
            logger.error(f"Ошибка при регистрации пользователя: {e}")
            return False

    def _determine_age_category(self, age: int) -> str:
        """Определение возрастной категории"""
        if age < 18:
            return "подросток"
        elif 18 <= age < 30:
            return "молодой взрослый"
        elif 30 <= age < 45:
            return "взрослый"
        elif 45 <= age < 60:
            return "средний возраст"
        else:
            return "пожилой"

    def get_user(self, user_id: int) -> Optional[Dict]:
        """Получить информацию о пользователе"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
                row = cursor.fetchone()
                
                if row:
                    columns = [description[0] for description in cursor.description]
                    return dict(zip(columns, row))
                return None
        except Exception as e:
            logger.error(f"Ошибка при получении пользователя: {e}")
            return None

    def update_user_activity(self, user_id: int) -> bool:
        """Обновить дату последней активности пользователя"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                UPDATE users 
                SET last_active_date = ?
                WHERE user_id = ?
                ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), user_id))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка при обновлении активности пользователя: {e}")
            return False

    def save_survey(
        self,
        user_id: int,
        bedtime: str,
        wakeup_time: str,
        sleep_duration: float,
        awakenings: int,
        sleep_quality: int,
        mood_morning: int,
        stress_level: int,
        exercise: int,
        caffeine: int,
        alcohol: int,
        screen_time: int,
        notes: str = ""
    ) -> bool:
        """Сохранить результаты опроса"""
        try:
            date = datetime.now().strftime('%Y-%m-%d')
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO surveys (
                    user_id, date, bedtime, wakeup_time, sleep_duration, 
                    awakenings, sleep_quality, mood_morning, stress_level, 
                    exercise, caffeine, alcohol, screen_time, notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    user_id, date, bedtime, wakeup_time, sleep_duration,
                    awakenings, sleep_quality, mood_morning, stress_level,
                    exercise, caffeine, alcohol, screen_time, notes
                ))
                
                # Проверяем достижения
                self._check_achievements(user_id, cursor)
                
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении опроса: {e}")
            return False

    def _check_achievements(self, user_id: int, cursor: sqlite3.Cursor):
        """Проверить и добавить достижения пользователя"""
        # Получаем количество завершенных опросов
        cursor.execute('SELECT COUNT(*) FROM surveys WHERE user_id = ?', (user_id,))
        survey_count = cursor.fetchone()[0]
        
        achievements = {
            10: "10 опросов",
            30: "30 опросов",
            50: "50 опросов",
            100: "100 опросов"
        }
        
        for count, achievement in achievements.items():
            if survey_count == count:
                # Проверяем, есть ли уже такое достижение
                cursor.execute('''
                SELECT COUNT(*) FROM achievements 
                WHERE user_id = ? AND achievement_type = ?
                ''', (user_id, achievement))
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                    INSERT INTO achievements (user_id, achievement_type, achievement_date)
                    VALUES (?, ?, ?)
                    ''', (user_id, achievement, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    def get_user_achievements(self, user_id: int) -> List[Dict]:
        """Получить достижения пользователя"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                SELECT achievement_type, achievement_date 
                FROM achievements 
                WHERE user_id = ? 
                ORDER BY achievement_date DESC
                ''', (user_id,))
                
                achievements = []
                for row in cursor.fetchall():
                    achievements.append({
                        'type': row[0],
                        'date': row[1]
                    })
                return achievements
        except Exception as e:
            logger.error(f"Ошибка при получении достижений: {e}")
            return []

    def save_recommendation(self, user_id: int, recommendation_text: str) -> bool:
        """Сохранить рекомендацию для пользователя"""
        try:
            date = datetime.now().strftime('%Y-%m-%d')
            
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                INSERT INTO recommendations (user_id, date, recommendation_text)
                VALUES (?, ?, ?)
                ''', (user_id, date, recommendation_text))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка при сохранении рекомендации: {e}")
            return False

    def update_recommendation_feedback(self, recommendation_id: int, is_helpful: bool) -> bool:
        """Обновить отзыв о рекомендации"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                UPDATE recommendations 
                SET is_helpful = ?, feedback_date = ?
                WHERE recommendation_id = ?
                ''', (int(is_helpful), datetime.now().strftime('%Y-%m-%d %H:%M:%S'), recommendation_id))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Ошибка при обновлении отзыва: {e}")
            return False

    def get_last_recommendation(self, user_id: int) -> Optional[Dict]:
        """Получить последнюю рекомендацию для пользователя"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                SELECT recommendation_id, recommendation_text, date 
                FROM recommendations 
                WHERE user_id = ? 
                ORDER BY date DESC 
                LIMIT 1
                ''', (user_id,))
                
                row = cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'text': row[1],
                        'date': row[2]
                    }
                return None
        except Exception as e:
            logger.error(f"Ошибка при получении рекомендации: {e}")
            return None

    def get_user_stats(self, user_id: int) -> Dict:
        """Получить статистику пользователя"""
        try:
            with self._get_connection() as conn:
                stats = {}
                
                # Основная информация о пользователе
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
                user_row = cursor.fetchone()
                
                if user_row:
                    columns = [description[0] for description in cursor.description]
                    stats['user_info'] = dict(zip(columns, user_row))
                
                # Статистика сна
                cursor.execute('''
                SELECT 
                    AVG(sleep_duration) as avg_sleep_duration,
                    AVG(sleep_quality) as avg_sleep_quality,
                    AVG(awakenings) as avg_awakenings,
                    COUNT(*) as total_surveys
                FROM surveys 
                WHERE user_id = ?
                ''', (user_id,))
                
                sleep_stats = cursor.fetchone()
                if sleep_stats:
                    stats['sleep_stats'] = {
                        'avg_sleep_duration': round(sleep_stats[0], 1) if sleep_stats[0] else 0,
                        'avg_sleep_quality': round(sleep_stats[1], 1) if sleep_stats[1] else 0,
                        'avg_awakenings': round(sleep_stats[2], 1) if sleep_stats[2] else 0,
                        'total_surveys': sleep_stats[3] if sleep_stats[3] else 0
                    }
                
                # Последние 7 записей сна
                cursor.execute('''
                SELECT date, sleep_duration, sleep_quality 
                FROM surveys 
                WHERE user_id = ? 
                ORDER BY date DESC 
                LIMIT 7
                ''', (user_id,))
                
                last_week = []
                for row in cursor.fetchall():
                    last_week.append({
                        'date': row[0],
                        'duration': row[1],
                        'quality': row[2]
                    })
                stats['last_week'] = last_week
                
                return stats
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return {}

    def get_all_users(self) -> List[int]:
        """Получить список всех пользователей"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT user_id FROM users')
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Ошибка при получении списка пользователей: {e}")
            return []

    def get_survey_data_for_analysis(self, user_id: int) -> pd.DataFrame:
        """Получить данные опросов для анализа"""
        try:
            with self._get_connection() as conn:
                query = '''
                SELECT 
                    bedtime, wakeup_time, sleep_duration, awakenings, 
                    sleep_quality, mood_morning, stress_level, exercise, 
                    caffeine, alcohol, screen_time
                FROM surveys 
                WHERE user_id = ?
                '''
                return pd.read_sql_query(query, conn, params=(user_id,))
        except Exception as e:
            logger.error(f"Ошибка при получении данных для анализа: {e}")
            return pd.DataFrame()

    def add_fact(self, fact_text: str, fact_type: str = "fact") -> bool:
        """Добавить новый факт или совет (для админа)"""
        try:
            # В этом упрощенном примере факты хранятся в отдельном файле
            # В реальном проекте можно добавить таблицу в БД
            with open('facts.py', 'a', encoding='utf-8') as f:
                if fact_type == "fact":
                    f.write(f'    "{fact_text}",\n')
                else:
                    f.write(f'    "{fact_text}",\n')
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении факта: {e}")
            return False

    def get_user_count(self) -> int:
        """Получить количество пользователей"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM users')
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Ошибка при получении количества пользователей: {e}")
            return 0
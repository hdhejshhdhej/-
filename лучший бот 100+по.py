BOT_TOKEN = "8359767158:AAGfShRDkHf9kVNCln1VVUxwWlPz0n6n7AE"
ADMIN_ID = 8063185389
PUBLIC_CHANNEL_ID = "-1003498880772"
ADMIN_CHANNEL_ID = "-1002310130501"

import telebot
from telebot import types
from tinydb import TinyDB, Query
from datetime import datetime, timedelta
import time
import os
import logging
import json
import shutil
import re

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================
# УЛУЧШЕННАЯ ФУНКЦИЯ ВОССТАНОВЛЕНИЯ ДАННЫХ
# ==============================================

def deep_recover_database(db_path):
    """Глубокое восстановление данных из поврежденной базы"""
    
    recovered_data = {
        'users': [],
        'tasks': [],
        'withdrawals': [],
        'admin_commands': [],
        'settings': [],
        'channels': [],
        'promo_codes': [],
        'bots': []
    }
    
    user_ids = set()  # Для избежания дубликатов
    
    try:
        if not os.path.exists(db_path):
            logger.warning(f"Файл базы данных не найден: {db_path}")
            return recovered_data
        
        # Создаем резервную копию
        backup_path = f"{db_path}.deep_backup"
        shutil.copy2(db_path, backup_path)
        logger.info(f"Создана резервная копия: {backup_path}")
        
        # Читаем файл целиком
        with open(db_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        logger.info(f"Размер файла: {len(content)} символов")
        
        # Метод 1: Поиск пользователей по их ID
        user_patterns = [
            r'"user_id"\s*:\s*(\d+)',
            r"'user_id'\s*:\s*(\d+)",
            r'user_id[\s=:]+(\d+)',
        ]
        
        all_user_ids = set()
        for pattern in user_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                try:
                    user_id = int(match)
                    if user_id > 0:
                        all_user_ids.add(user_id)
                except:
                    continue
        
        logger.info(f"Найдено {len(all_user_ids)} уникальных ID пользователей")
        
        # Метод 2: Поиск JSON объектов пользователей
        # Ищем блоки с данными пользователей
        user_blocks = []
        
        # Паттерны для поиска пользовательских данных
        patterns = [
            r'\{[^{}]*"user_id"[^{}]*\}',  # Простые объекты
            r'\{[^{}]*"user_id"[^{}]*\{[^{}]*\}[^{}]*\}',  # Объекты с вложенностями
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            user_blocks.extend(matches)
        
        # Также ищем по более широкому паттерну
        all_blocks = re.findall(r'\{[^{}]*\}', content, re.DOTALL)
        user_blocks.extend([b for b in all_blocks if '"user_id"' in b or "'user_id'" in b])
        
        # Обрабатываем найденные блоки
        for block in user_blocks:
            try:
                # Очищаем блок от возможного мусора
                clean_block = block.strip()
                
                # Пробуем разные подходы к парсингу
                try:
                    user_data = json.loads(clean_block)
                except:
                    # Пробуем исправить JSON
                    # Заменяем одинарные кавычки на двойные
                    fixed_block = clean_block.replace("'", '"')
                    # Исправляем запятые в конце объектов
                    fixed_block = re.sub(r',\s*}', '}', fixed_block)
                    fixed_block = re.sub(r',\s*]', ']', fixed_block)
                    
                    try:
                        user_data = json.loads(fixed_block)
                    except:
                        # Пробуем извлечь данные через регулярки
                        user_data = {}
                        
                        # Извлекаем user_id
                        uid_match = re.search(r'"user_id"\s*:\s*(\d+)', fixed_block)
                        if uid_match:
                            user_data['user_id'] = int(uid_match.group(1))
                        
                        # Извлекаем balance
                        balance_match = re.search(r'"balance"\s*:\s*(\d+)', fixed_block)
                        if balance_match:
                            user_data['balance'] = int(balance_match.group(1))
                        else:
                            user_data['balance'] = 0
                        
                        # Извлекаем first_name
                        name_match = re.search(r'"first_name"\s*:\s*"([^"]+)"', fixed_block)
                        if name_match:
                            user_data['first_name'] = name_match.group(1)
                        else:
                            user_data['first_name'] = 'Пользователь'
                        
                        # Извлекаем username
                        username_match = re.search(r'"username"\s*:\s*"([^"]+)"', fixed_block)
                        if username_match:
                            user_data['username'] = username_match.group(1)
                        else:
                            user_data['username'] = f"user_{user_data.get('user_id', 'unknown')}"
                
                # Проверяем, что это действительно пользователь
                if 'user_id' in user_data:
                    user_id = user_data['user_id']
                    
                    if user_id in user_ids:
                        continue  # Пропускаем дубликаты
                    
                    user_ids.add(user_id)
                    
                    # Создаем полную запись пользователя
                    user_record = {
                        'user_id': user_id,
                        'balance': user_data.get('balance', 0),
                        'first_name': user_data.get('first_name', 'Пользователь'),
                        'last_name': user_data.get('last_name', ''),
                        'username': user_data.get('username', f"user_{user_id}"),
                        'referrer_id': user_data.get('referrer_id'),
                        'completed_tasks': user_data.get('completed_tasks', []),
                        'referrals': user_data.get('referrals', []),
                        'registration_date': user_data.get('registration_date', datetime.now().isoformat()),
                        'frozen': user_data.get('frozen', False),
                        'used_promo_codes': user_data.get('used_promo_codes', []),
                        'referral_rewarded': user_data.get('referral_rewarded', False)
                    }
                    
                    recovered_data['users'].append(user_record)
                    logger.debug(f"Восстановлен пользователь ID: {user_id}")
                    
            except Exception as e:
                continue
        
        # Метод 3: Попробуем найти полные JSON структуры TinyDB
        # Ищем таблицу users
        users_table_pattern = r'"users"\s*:\s*\{[^}]*\}'
        users_match = re.search(users_table_pattern, content, re.DOTALL)
        
        if users_match:
            users_table_content = users_match.group(0)
            # Ищем отдельные записи пользователей
            user_entries = re.findall(r'"\d+"\s*:\s*\{[^}]*\}', users_table_content, re.DOTALL)
            
            for entry in user_entries:
                try:
                    # Извлекаем JSON объекта
                    entry_match = re.search(r'\{[^}]*\}', entry, re.DOTALL)
                    if entry_match:
                        user_json = entry_match.group(0)
                        user_data = json.loads(user_json)
                        
                        if 'user_id' in user_data:
                            user_id = user_data['user_id']
                            
                            if user_id not in user_ids:
                                user_ids.add(user_id)
                                
                                user_record = {
                                    'user_id': user_id,
                                    'balance': user_data.get('balance', 0),
                                    'first_name': user_data.get('first_name', 'Пользователь'),
                                    'last_name': user_data.get('last_name', ''),
                                    'username': user_data.get('username', f"user_{user_id}"),
                                    'referrer_id': user_data.get('referrer_id'),
                                    'completed_tasks': user_data.get('completed_tasks', []),
                                    'referrals': user_data.get('referrals', []),
                                    'registration_date': user_data.get('registration_date', datetime.now().isoformat()),
                                    'frozen': user_data.get('frozen', False),
                                    'used_promo_codes': user_data.get('used_promo_codes', []),
                                    'referral_rewarded': user_data.get('referral_rewarded', False)
                                }
                                
                                recovered_data['users'].append(user_record)
                                logger.debug(f"Из таблицы users восстановлен ID: {user_id}")
                except:
                    continue
        
        # Если все еще мало пользователей, используем агрессивный поиск
        if len(recovered_data['users']) < 30:
            logger.info("Используем агрессивный поиск пользователей...")
            
            # Ищем все числа, которые могут быть user_id
            potential_ids = re.findall(r'\b\d{7,10}\b', content)
            
            for pid in potential_ids:
                try:
                    user_id = int(pid)
                    if user_id > 1000000 and user_id not in user_ids:  # Telegram ID обычно большие
                        # Ищем данные вокруг этого ID
                        pattern = rf'.{{0,500}}{user_id}.{{0,500}}'
                        context_match = re.search(pattern, content, re.DOTALL)
                        
                        if context_match:
                            context = context_match.group(0)
                            
                            # Проверяем, что это похоже на пользователя
                            if 'balance' in context or 'username' in context or 'first_name' in context:
                                user_ids.add(user_id)
                                
                                user_record = {
                                    'user_id': user_id,
                                    'balance': 0,
                                    'first_name': 'Пользователь',
                                    'last_name': '',
                                    'username': f"user_{user_id}",
                                    'referrer_id': None,
                                    'completed_tasks': [],
                                    'referrals': [],
                                    'registration_date': datetime.now().isoformat(),
                                    'frozen': False,
                                    'used_promo_codes': [],
                                    'referral_rewarded': False
                                }
                                
                                # Пробуем извлечь баланс
                                balance_match = re.search(r'"balance"\s*:\s*(\d+)', context)
                                if balance_match:
                                    user_record['balance'] = int(balance_match.group(1))
                                
                                # Пробуем извлечь имя
                                name_match = re.search(r'"first_name"\s*:\s*"([^"]+)"', context)
                                if name_match:
                                    user_record['first_name'] = name_match.group(1)
                                
                                # Пробуем извлечь username
                                username_match = re.search(r'"username"\s*:\s*"([^"]+)"', context)
                                if username_match:
                                    user_record['username'] = username_match.group(1)
                                
                                recovered_data['users'].append(user_record)
                except:
                    continue
        
        # Восстанавливаем другие таблицы
        logger.info("Восстанавливаем другие таблицы...")
        
        # Задания
        task_patterns = [
            r'"tasks"\s*:\s*\{[^}]*\}',
            r'"channel_id"[^}]+"link"[^}]+"reward"',
        ]
        
        for pattern in task_patterns:
            matches = re.findall(pattern, content, re.DOTALL)
            for match in matches:
                try:
                    # Ищем отдельные задания
                    task_blocks = re.findall(r'\{[^}]*"channel_id"[^}]*"link"[^}]*"reward"[^}]*\}', match, re.DOTALL)
                    
                    for block in task_blocks:
                        try:
                            task_data = json.loads(block)
                            task = {
                                'channel_id': task_data.get('channel_id', ''),
                                'link': task_data.get('link', ''),
                                'reward': task_data.get('reward', 1)
                            }
                            if task not in recovered_data['tasks']:
                                recovered_data['tasks'].append(task)
                        except:
                            continue
                except:
                    continue
        
        # Каналы
        channel_pattern = r'"channels"\s*:\s*\{[^}]*\}'
        channel_match = re.search(channel_pattern, content, re.DOTALL)
        if channel_match:
            try:
                channels_content = channel_match.group(0)
                channel_blocks = re.findall(r'\{[^}]*"channel_id"[^}]*"link"[^}]*\}', channels_content, re.DOTALL)
                
                for block in channel_blocks:
                    try:
                        channel_data = json.loads(block)
                        channel = {
                            'channel_id': channel_data.get('channel_id', ''),
                            'link': channel_data.get('link', ''),
                            'name': channel_data.get('name', 'Канал')
                        }
                        if channel not in recovered_data['channels']:
                            recovered_data['channels'].append(channel)
                    except:
                        continue
            except:
                pass
        
        # Настройки
        settings_pattern = r'"settings"\s*:\s*\{[^}]*\}'
        settings_match = re.search(settings_pattern, content, re.DOTALL)
        if settings_match:
            try:
                settings_content = settings_match.group(0)
                settings_block = re.search(r'\{[^}]*\}', settings_content, re.DOTALL)
                if settings_block:
                    settings_data = json.loads(settings_block.group(0))
                    settings = {
                        'min_referrals': settings_data.get('min_referrals', 5),
                        'min_tasks': settings_data.get('min_tasks', 3),
                        'referral_reward': settings_data.get('referral_reward', 1)
                    }
                    recovered_data['settings'].append(settings)
            except:
                pass
        
        logger.info(f"✅ Глубокое восстановление завершено:")
        logger.info(f"   👥 Пользователей: {len(recovered_data['users'])}")
        logger.info(f"   📝 Заданий: {len(recovered_data['tasks'])}")
        logger.info(f"   📢 Каналов: {len(recovered_data['channels'])}")
        logger.info(f"   ⚙️ Настроек: {len(recovered_data['settings'])}")
        
        # Сортируем пользователей по ID для удобства
        recovered_data['users'].sort(key=lambda x: x['user_id'])
        
        return recovered_data
        
    except Exception as e:
        logger.error(f"❌ Ошибка глубокого восстановления: {e}")
        import traceback
        traceback.print_exc()
        return recovered_data

def create_deep_recovered_database(db_path, recovered_data):
    """Создание новой базы данных из глубоко восстановленных данных"""
    
    # Структура TinyDB
    new_db = {
        '_default': {'1': {'_type': 'document', '_id': 1}},
        'users': {},
        'tasks': {},
        'withdrawals': {},
        'admin_commands': {},
        'settings': {},
        'channels': {},
        'promo_codes': {},
        'bots': {}
    }
    
    # Добавляем пользователей
    user_count = 0
    for idx, user in enumerate(recovered_data['users'], 1):
        new_db['users'][str(idx)] = user
        user_count += 1
    
    # Добавляем задания
    for idx, task in enumerate(recovered_data['tasks'], 1):
        new_db['tasks'][str(idx)] = task
    
    # Добавляем каналы
    for idx, channel in enumerate(recovered_data['channels'], 1):
        new_db['channels'][str(idx)] = channel
    
    # Добавляем настройки
    if recovered_data['settings']:
        new_db['settings']['1'] = recovered_data['settings'][0]
    else:
        new_db['settings']['1'] = {
            'min_referrals': 5,
            'min_tasks': 3,
            'referral_reward': 1
        }
    
    # Сохраняем
    with open(db_path, 'w', encoding='utf-8') as f:
        json.dump(new_db, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✅ Создана новая база данных с {user_count} пользователями")
    return True

# ==============================================
# УЛУЧШЕННЫЙ КЛАСС БАЗЫ ДАННЫХ С ГЛУБОКИМ ВОССТАНОВЛЕНИЕМ
# ==============================================

class DeepRecoveryTinyDB:
    """База данных с глубоким восстановлением"""
    
    def __init__(self, path):
        self.path = path
        self.db = None
        self._initialize_with_deep_recovery()
    
    def _initialize_with_deep_recovery(self):
        """Инициализация с глубоким восстановлением"""
        
        logger.info("🔍 Начинаем глубокое восстановление базы данных...")
        
        # Сначала пробуем стандартную загрузку
        try:
            if os.path.exists(self.path):
                with open(self.path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self.db = TinyDB(self.path, encoding='utf-8')
                users_count = len(self.db.table('users').all())
                logger.info(f"✅ База загружена: {users_count} пользователей")
                return True
        except:
            pass
        
        # Если стандартная загрузка не удалась, делаем глубокое восстановление
        logger.info("🔄 База повреждена, начинаем глубокое восстановление...")
        
        # Сохраняем оригинальный файл
        if os.path.exists(self.path):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            original_backup = f"{self.path}.original_{timestamp}"
            shutil.copy2(self.path, original_backup)
            logger.info(f"💾 Оригинальный файл сохранен как: {original_backup}")
        
        # Выполняем глубокое восстановление
        recovered_data = deep_recover_database(self.path)
        
        if recovered_data['users']:
            logger.info(f"📊 Результаты восстановления:")
            logger.info(f"   👥 Найдено пользователей: {len(recovered_data['users'])}")
            
            # Показываем первых 20 пользователей
            logger.info("📋 Первые 20 восстановленных пользователей:")
            for i, user in enumerate(recovered_data['users'][:20]):
                logger.info(f"   {i+1}. ID: {user['user_id']}, Имя: {user['first_name']}, Баланс: {user['balance']}")
            
            # Создаем новую базу
            temp_path = f"{self.path}.deep_recovered"
            create_deep_recovered_database(temp_path, recovered_data)
            
            # Заменяем старый файл
            if os.path.exists(self.path):
                corrupted_backup = f"{self.path}.corrupted_{timestamp}"
                shutil.move(self.path, corrupted_backup)
                logger.info(f"🗑️ Поврежденный файл перемещен в: {corrupted_backup}")
            
            shutil.move(temp_path, self.path)
            logger.info("✅ База данных восстановлена!")
            
            # Загружаем восстановленную базу
            try:
                self.db = TinyDB(self.path, encoding='utf-8')
                users_count = len(self.db.table('users').all())
                logger.info(f"✅ Восстановленная база загружена: {users_count} пользователей")
                return True
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки восстановленной базы: {e}")
        
        # Если восстановление не удалось, создаем пустую базу
        logger.warning("⚠️ Не удалось восстановить данные, создаем новую базу")
        return self._create_empty_database()
    
    def _create_empty_database(self):
        """Создание пустой базы данных"""
        try:
            empty_db = {
                '_default': {'1': {'_type': 'document', '_id': 1}},
                'users': {},
                'tasks': {},
                'withdrawals': {},
                'admin_commands': {},
                'settings': {},
                'channels': {},
                'promo_codes': {},
                'bots': {}
            }
            
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(empty_db, f, ensure_ascii=False, indent=2)
            
            self.db = TinyDB(self.path, encoding='utf-8')
            logger.info("🆕 Создана новая пустая база данных")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка создания новой базы: {e}")
            return False
    
    def table(self, name):
        """Получение таблицы"""
        if self.db is None:
            self._initialize_with_deep_recovery()
        return self.db.table(name)
    
    def close(self):
        """Закрытие базы данных"""
        if self.db:
            self.db.close()

# ==============================================
# ОСНОВНОЙ КОД БОТА (остается без изменений)
# ==============================================

# Инициализация бота
bot = telebot.TeleBot(BOT_TOKEN)

# Создаем базу данных с глубоким восстановлением
db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'baseоповарп2n.json')
db = DeepRecoveryTinyDB(db_path)

# Определение запросов для таблиц
User = Query()
Task = Query()
Withdrawal = Query()
AdminCommand = Query()
Settings = Query()
Channel = Query()
Promo = Query()
Bot = Query()

# Создание таблиц
users_table = db.table('users')
tasks_table = db.table('tasks')
withdrawals_table = db.table('withdrawals')
admin_commands_table = db.table('admin_commands')
settings_table = db.table('settings')
channels_table = db.table('channels')
promo_table = db.table('promo_codes')
bots_table = db.table('bots')

BOT_USERNAME = bot.get_me().username
STICKER_ID = "CAACAgIAAxkBAAEN649nwo9L9MNUgBKoEBRyTHSlL80rTwAC_xAAApc0QEukJ1CkXhMnSDYE"

# Инициализация настроек по умолчанию
if not settings_table.all():
    settings_table.insert({
        'min_referrals': 5,
        'min_tasks': 3,
        'referral_reward': 1
    })

# ==============================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ==============================================

def check_subscription(user_id):
    """Проверяет, подписан ли пользователь на обязательные каналы"""
    try:
        channels = channels_table.all()
        
        # Если нет обязательных каналов, доступ разрешен
        if not channels:
            return True
            
        # Проверяем каналы (реальная проверка через get_chat_member)
        for channel in channels:
            try:
                chat_member = bot.get_chat_member(channel['channel_id'], user_id)
                if chat_member.status not in ['member', 'administrator', 'creator']:
                    return False
            except Exception as e:
                logger.error(f"Ошибка проверки подписки на канал {channel['channel_id']}: {e}")
                return False
                
        return True
    except Exception as e:
        logger.error(f"Ошибка в check_subscription: {e}")
        return False

def show_subscription_message(chat_id, user_id):
    """Показывает сообщение с обязательными подписками"""
    try:
        channels = channels_table.all()
        bots = bots_table.all()
        
        if not channels and not bots:
            return True
            
        markup = types.InlineKeyboardMarkup()
        message_text = "📋 <b>Для доступа к боту необходимо подписаться на все каналы</b>\n\n"
        
        # Добавляем каналы (реальная проверка)
        for index, channel in enumerate(channels, 1):
            channel_link = channel['link']
            btn = types.InlineKeyboardButton(f"📢 Канал {index}", url=channel_link)
            markup.add(btn)
        
        # Добавляем боты (просто кнопки без проверки)
        for index, bot_data in enumerate(bots, 1):
            bot_link = f"https://t.me/{bot_data['username']}"
            btn = types.InlineKeyboardButton(f"🤖 Бот {index}", url=bot_link)
            markup.add(btn)
        
        markup.add(types.InlineKeyboardButton("✅ Я подписался", callback_data="check_subscription"))
        
        message_text += "После подписки нажмите кнопку '✅ Я подписался' для проверки."
        bot.send_message(chat_id, message_text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Ошибка в show_subscription_message: {e}")

def check_user_id_for_twin(user_id):
    """Проверяет ID пользователя на твинк-аккаунт"""
    user_id_str = str(user_id)
    
    # Определяем первые 2 цифры ID
    if len(user_id_str) >= 2:
        first_two_digits = int(user_id_str[:2])
        return first_two_digits >= 75
    return False

def reward_referrer(user_id):
    """Награждает реферера после успешной проверки подписки"""
    try:
        user_data = users_table.search(User.user_id == user_id)
        if not user_data:
            return
        
        user_data = user_data[0]
        referrer_id = user_data.get('referrer_id')
        
        # Проверяем, есть ли реферер и не было ли уже награждения
        if referrer_id and referrer_id != str(user_id) and not user_data.get('referral_rewarded', False):
            referrer = users_table.search(User.user_id == int(referrer_id))
            if referrer and not referrer[0].get('frozen', False):
                # ПРОВЕРКА НА ТВИНК-АККАУНТ
                if check_user_id_for_twin(user_id):
                    # Отправляем сообщение рефереру о твинк-аккаунте
                    bot.send_message(
                        referrer_id,
                        "⚠️ Новый пользователь по твоей ссылке - это новый аккаунт или ваш твинк.\n"
                        "Награда за него не начисляется!⚠️ Награда не зачислена.Согласно правилам (https://t.me/tiktokm0neys), награда за таких пользователей не начисляется.",
                        parse_mode='HTML'
                    )
                    # Помечаем, что реферал проверен (но не награжден)
                    users_table.update({
                        'referral_rewarded': True
                    }, User.user_id == user_id)
                    return
                
                # Если не твинк, начисляем награду
                settings = settings_table.all()[0]
                reward = settings.get('referral_reward', 1)
                new_balance = referrer[0]['balance'] + reward
                referrer_data = referrer[0]
                
                # Обновляем список рефералов реферера
                referrals = referrer_data.get('referrals', [])
                if user_id not in referrals:
                    referrals.append(user_id)
                
                # Обновляем баланс и список рефералов реферера
                users_table.update({
                    'balance': new_balance, 
                    'referrals': referrals
                }, User.user_id == int(referrer_id))
                
                # Помечаем, что реферал награжден
                users_table.update({
                    'referral_rewarded': True
                }, User.user_id == user_id)
                
                # Отправляем уведомление рефереру
                bot.send_message(
                    referrer_id, 
                    f"🎉 Новый пользователь по твоей ссылке прошел проверку! +{reward} звезд ⭐️\n"
                    f"Твой баланс: {new_balance} звезд ⭐️", 
                    parse_mode='HTML'
                )
    except Exception as e:
        logger.error(f"Ошибка в reward_referrer: {e}")

def show_main_menu(chat_id):
    try:
        markup = types.InlineKeyboardMarkup()
        btn_referral = types.InlineKeyboardButton("Заработать звезды", callback_data="referral")
        btn_profile = types.InlineKeyboardButton("Профиль", callback_data="profile")
        btn_withdraw = types.InlineKeyboardButton("Вывести звезды", callback_data="withdraw")
        btn_tasks = types.InlineKeyboardButton("Задания", callback_data="tasks_1")
        btn_promo = types.InlineKeyboardButton("Промокод", callback_data="promo")
        btn_top = types.InlineKeyboardButton("Топ", callback_data="top_day")
        
        markup.add(btn_referral)
        markup.row(btn_profile, btn_withdraw)
        markup.row(btn_tasks, btn_promo)
        markup.add(btn_top)
        
        bot.send_message(chat_id, "<b>Главное меню</b>", reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Ошибка в show_main_menu: {e}")

def main_menu_markup():
    markup = types.InlineKeyboardMarkup()
    btn_referral = types.InlineKeyboardButton("Заработать звезды", callback_data="referral")
    btn_profile = types.InlineKeyboardButton("Профиль", callback_data="profile")
    btn_withdraw = types.InlineKeyboardButton("Вывести звезды", callback_data="withdraw")
    btn_tasks = types.InlineKeyboardButton("Задания", callback_data="tasks_1")
    btn_promo = types.InlineKeyboardButton("Промокод", callback_data="promo")
    btn_top = types.InlineKeyboardButton("Топ", callback_data="top_day")
    
    markup.add(btn_referral)
    markup.row(btn_profile, btn_withdraw)
    markup.row(btn_tasks, btn_promo)
    markup.add(btn_top)
    
    return markup

# ==============================================
# ОБРАБОТЧИКИ СООБЩЕНИЙ (полная версия)
# ==============================================

@bot.message_handler(commands=['start'])
def send_welcome(message):
    user_id = message.from_user.id
    
    try:
        # Отправляем стикер
        bot.send_sticker(message.chat.id, STICKER_ID)
        
        # Обработка реферальной ссылки
        args = message.text.split()
        if len(args) > 1:
            ref_code = args[1]
            
            # Это обычный реферальный код
            referrer_id = ref_code
            
            if not users_table.search(User.user_id == user_id):
                users_table.insert({
                    'user_id': user_id,
                    'balance': 0,
                    'first_name': message.from_user.first_name,
                    'last_name': message.from_user.last_name if message.from_user.last_name else '',
                    'referrer_id': referrer_id,
                    'completed_tasks': [],
                    'registration_date': datetime.now().isoformat(),
                    'referrals': [],
                    'username': message.from_user.username or "NoUsername",
                    'frozen': False,
                    'used_promo_codes': [],
                    'referral_rewarded': False
                })
            else:
                # Если пользователь уже существует, но пришел по реферальной ссылке
                user_data = users_table.search(User.user_id == user_id)
                if user_data and not user_data[0].get('referrer_id'):
                    users_table.update({
                        'referrer_id': referrer_id,
                        'referral_rewarded': False
                    }, User.user_id == user_id)
        
        # Проверяем подписку только на каналы
        if not check_subscription(user_id):
            show_subscription_message(message.chat.id, user_id)
            return
        
        # Если пользователь еще не зарегистрирован (пришел без реферальной ссылки)
        if not users_table.search(User.user_id == user_id):
            users_table.insert({
                'user_id': user_id,
                'balance': 0,
                'first_name': message.from_user.first_name,
                'last_name': message.from_user.last_name if message.from_user.last_name else '',
                'referrer_id': None,
                'completed_tasks': [],
                'registration_date': datetime.now().isoformat(),
                'referrals': [],
                'username': message.from_user.username or "NoUsername",
                'frozen': False,
                'used_promo_codes': [],
                'referral_rewarded': False
            })
        else:
            # Пользователь уже существует - награждаем реферера если нужно
            reward_referrer(user_id)

        # Показываем главное меню
        show_main_menu(message.chat.id)
    except Exception as e:
        logger.error(f"Ошибка в send_welcome: {e}")

@bot.message_handler(commands=['admin'])
def admin_panel(message):
    user_id = message.from_user.id
    if user_id != ADMIN_ID:
        return
    
    try:
        markup = types.InlineKeyboardMarkup()
        btn_stats = types.InlineKeyboardButton("Статистика", callback_data="admin_stats")
        btn_task_stats = types.InlineKeyboardButton("Статистика заданий", callback_data="admin_task_stats_1")
        btn_users = types.InlineKeyboardButton("Пользователи", callback_data="admin_users_1")
        btn_add_task = types.InlineKeyboardButton("Добавить задание", callback_data="admin_add")
        btn_delete_task = types.InlineKeyboardButton("Удалить задание", callback_data="admin_delete")
        btn_add_channel = types.InlineKeyboardButton("Добавить канал", callback_data="admin_add_channel")
        btn_delete_channel = types.InlineKeyboardButton("Удалить канал", callback_data="admin_delete_channel")
        btn_add_bot = types.InlineKeyboardButton("Добавить бота", callback_data="admin_add_bot")
        btn_delete_bot = types.InlineKeyboardButton("Удалить бота", callback_data="admin_delete_bot")
        btn_subscription_stats = types.InlineKeyboardButton("Статистика подписок", callback_data="admin_subscription_stats")
        btn_add_promo = types.InlineKeyboardButton("Создать промокод", callback_data="admin_add_promo")
        btn_delete_promo = types.InlineKeyboardButton("Удалить промокод", callback_data="admin_delete_promo")
        btn_set_min_refs = types.InlineKeyboardButton("Установить мин. рефералов", callback_data="admin_set_min_refs")
        btn_set_min_tasks = types.InlineKeyboardButton("Установить мин. заданий", callback_data="admin_set_min_tasks")
        btn_set_ref_reward = types.InlineKeyboardButton("Установить награду за реферала", callback_data="admin_set_ref_reward")
        btn_freeze = types.InlineKeyboardButton("Заморозить", callback_data="admin_freeze")
        btn_unfreeze = types.InlineKeyboardButton("Разморозить", callback_data="admin_unfreeze")
        btn_reset = types.InlineKeyboardButton("Обнулить аккаунт", callback_data="admin_reset")
        btn_broadcast = types.InlineKeyboardButton("Рассылка", callback_data="admin_broadcast")
        
        markup.add(btn_stats, btn_task_stats)
        markup.add(btn_users)
        markup.add(btn_add_task, btn_delete_task)
        markup.add(btn_add_channel, btn_delete_channel)
        markup.add(btn_add_bot, btn_delete_bot)
        markup.add(btn_subscription_stats)
        markup.add(btn_add_promo, btn_delete_promo)
        markup.add(btn_set_min_refs, btn_set_min_tasks)
        markup.add(btn_set_ref_reward)
        markup.add(btn_freeze, btn_unfreeze)
        markup.add(btn_reset)
        markup.add(btn_broadcast)
        
        bot.reply_to(message, "<b>Админ-панель</b>", reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Ошибка в admin_panel: {e}")

@bot.message_handler(func=lambda message: message.from_user.id == ADMIN_ID)
def handle_admin_input(message):
    user_id = message.from_user.id
    admin_command = admin_commands_table.search(AdminCommand.user_id == user_id)
    
    if not admin_command:
        return
    
    command = admin_command[0]
    step = command.get('step')
    
    try:
        if step == 'channel_id':
            admin_commands_table.update({'channel_id': message.text, 'step': 'link'}, AdminCommand.user_id == user_id)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.reply_to(message, "Введите ссылку на канал:", reply_markup=markup)
        
        elif step == 'link':
            admin_commands_table.update({'link': message.text, 'step': 'channel_name'}, AdminCommand.user_id == user_id)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.reply_to(message, "Введите название канала (для отображения):", reply_markup=markup)
        
        elif step == 'channel_name':
            channels_table.insert({
                'channel_id': command['channel_id'],
                'link': command['link'],
                'name': message.text
            })
            bot.reply_to(message, "Канал добавлен в обязательные подписки!")
            admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'delete_channel':
            try:
                channel_num = int(message.text) - 1
                all_channels = channels_table.all()
                if 0 <= channel_num < len(all_channels):
                    channels_table.remove(doc_ids=[all_channels[channel_num].doc_id])
                    bot.reply_to(message, "Канал удален!")
                else:
                    bot.reply_to(message, "Ошибка! Нет такого номера канала.")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'bot_username':
            admin_commands_table.update({'username': message.text, 'step': 'bot_name'}, AdminCommand.user_id == user_id)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.reply_to(message, "Введите имя бота (для отображения):", reply_markup=markup)
        
        elif step == 'bot_name':
            bots_table.insert({
                'username': command['username'],
                'name': message.text
            })
            bot.reply_to(message, "Бот добавлен в обязательные подписки!")
            admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'delete_bot':
            try:
                bot_num = int(message.text) - 1
                all_bots = bots_table.all()
                if 0 <= bot_num < len(all_bots):
                    bots_table.remove(doc_ids=[all_bots[bot_num].doc_id])
                    bot.reply_to(message, "Бот удален!")
                else:
                    bot.reply_to(message, "Ошибка! Нет такого номера бота.")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'task_channel_id':
            admin_commands_table.update({'channel_id': message.text, 'step': 'task_link'}, AdminCommand.user_id == user_id)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.reply_to(message, "Введите ссылку на канал:", reply_markup=markup)
        
        elif step == 'task_link':
            admin_commands_table.update({'link': message.text, 'step': 'reward'}, AdminCommand.user_id == user_id)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.reply_to(message, "Сколько звезд давать за задание?", reply_markup=markup)
        
        elif step == 'reward':
            try:
                reward = int(message.text)
                tasks_table.insert({
                    'channel_id': command['channel_id'],
                    'link': command['link'],
                    'reward': reward
                })
                bot.reply_to(message, "Задание добавлено!")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'delete_task':
            try:
                task_num = int(message.text) - 1
                all_tasks = tasks_table.all()
                if 0 <= task_num < len(all_tasks):
                    tasks_table.remove(doc_ids=[all_tasks[task_num].doc_id])
                    bot.reply_to(message, "Задание удалено!")
                else:
                    bot.reply_to(message, "Ошибка! Нет такого номера задания.")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'set_min_refs':
            try:
                new_min_refs = int(message.text)
                if new_min_refs < 0:
                    raise ValueError
                settings_table.update({'min_referrals': new_min_refs}, Settings.min_referrals.exists())
                bot.reply_to(message, f"Минимальное количество рефералов теперь: {new_min_refs}")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите положительное число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'set_min_tasks':
            try:
                new_min_tasks = int(message.text)
                if new_min_tasks < 0:
                    raise ValueError
                settings_table.update({'min_tasks': new_min_tasks}, Settings.min_tasks.exists())
                bot.reply_to(message, f"Минимальное количество заданий теперь: {new_min_tasks}")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите положительное число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'set_ref_reward':
            try:
                reward = int(message.text)
                if reward < 0:
                    raise ValueError
                settings_table.update({'referral_reward': reward}, Settings.referral_reward.exists())
                bot.reply_to(message, f"Награда за реферала теперь: {reward} звезд ⭐️")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите положительное число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'promo_code':
            admin_commands_table.update({'code': message.text, 'step': 'promo_reward'}, AdminCommand.user_id == user_id)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.reply_to(message, "Введите сколько будет даваться звезд за активацию промокода:", reply_markup=markup)
        
        elif step == 'promo_reward':
            try:
                reward = int(message.text)
                admin_commands_table.update({'reward': reward, 'step': 'promo_limit'}, AdminCommand.user_id == user_id)
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
                bot.reply_to(message, "Сколько пользователей могут активировать промокод:", reply_markup=markup)
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
        
        elif step == 'promo_limit':
            try:
                limit = int(message.text)
                promo_table.insert({
                    'code': command['code'],
                    'reward': command['reward'],
                    'limit': limit,
                    'used_by': []
                })
                bot.reply_to(message, f"Промокод {command['code']} создан!")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'delete_promo':
            try:
                promo_num = int(message.text) - 1
                all_promos = promo_table.all()
                if 0 <= promo_num < len(all_promos):
                    promo_table.remove(doc_ids=[all_promos[promo_num].doc_id])
                    bot.reply_to(message, "Промокод удален!")
                else:
                    bot.reply_to(message, "Ошибка! Нет такого номера промокода.")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'freeze':
            try:
                target_id = int(message.text)
                if users_table.search(User.user_id == target_id):
                    users_table.update({'frozen': True}, User.user_id == target_id)
                    bot.reply_to(message, f"Пользователь {target_id} заморожен!")
                else:
                    bot.reply_to(message, "Ошибка! Пользователь не найден.")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'unfreeze':
            try:
                target_id = int(message.text)
                if users_table.search(User.user_id == target_id):
                    users_table.update({'frozen': False}, User.user_id == target_id)
                    bot.reply_to(message, f"Пользователь {target_id} разморожен!")
                else:
                    bot.reply_to(message, "Ошибка! Пользователь не найден.")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'reset':
            try:
                target_id = int(message.text)
                if users_table.search(User.user_id == target_id):
                    users_table.update({
                        'balance': 0,
                        'completed_tasks': [],
                        'referrals': [],
                        'used_promo_codes': [],
                        'frozen': False,
                        'referral_rewarded': False
                    }, User.user_id == target_id)
                    bot.reply_to(message, f"Аккаунт пользователя {target_id} обнулен!")
                else:
                    bot.reply_to(message, "Ошибка! Пользователь не найден.")
            except ValueError:
                bot.reply_to(message, "Ошибка! Введите число.")
            finally:
                admin_commands_table.remove(AdminCommand.user_id == user_id)
        
        elif step == 'broadcast':
            users = users_table.all()
            success_count = 0
            fail_count = 0
            
            bot.reply_to(message, f"Начинаю рассылку сообщения для {len(users)} пользователей...")
            
            for user in users:
                try:
                    bot.send_message(user['user_id'], message.text)
                    success_count += 1
                except Exception as e:
                    logger.error(f"Ошибка отправки сообщения пользователю {user['user_id']}:{e}")
                    fail_count += 1
                time.sleep(0.1)
            
            report = f"Рассылка завершена!\nУспешно: {success_count}\nНе удалось: {fail_count}"
            bot.send_message(user_id, report)
            
            admin_commands_table.remove(AdminCommand.user_id == user_id)
                
    except Exception as e:
        logger.error(f"Ошибка в handle_admin_input: {e}")
        bot.reply_to(message, "Произошла ошибка при обработке команды.")

@bot.message_handler(func=lambda message: True)
def handle_promo_input(message):
    user_id = message.from_user.id
    user_data = users_table.search(User.user_id == user_id)
    
    if not user_data or admin_commands_table.search(AdminCommand.user_id == user_id):
        return
    
    try:
        promo = promo_table.search(Promo.code == message.text)
        if not promo:
            bot.reply_to(message, "Промокод не найден!")
            return
        
        promo = promo[0]
        used_by = promo.get('used_by', [])
        if user_id in used_by:
            bot.reply_to(message, "Ты уже активировал этот промокод!")
            return
        
        if len(used_by) >= promo['limit']:
            bot.reply_to(message, "Лимит активаций промокода исчерпан!")
            return
        
        if user_data[0].get('frozen', False):
            bot.reply_to(message, "Твой аккаунт заморожен, активация невозможна!")
            return
        
        used_promo_codes = user_data[0].get('used_promo_codes', [])
        used_promo_codes.append(message.text)
        new_balance = user_data[0]['balance'] + promo['reward']
        used_by.append(user_id)
        
        users_table.update({'balance': new_balance, 'used_promo_codes': used_promo_codes}, User.user_id == user_id)
        promo_table.update({'used_by': used_by}, Promo.code == message.text)
        bot.reply_to(message, f"Промокод активирован! Ты получил {promo['reward']} звезд ⭐️\nТвой баланс: {new_balance} звезд ⭐️")
    except Exception as e:
        logger.error(f"Ошибка в handle_promo_input: {e}")

# ==============================================
# ОБРАБОТЧИКИ КНОПОК (CALLBACK QUERY) - полная версия
# ==============================================

@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    user_id = call.from_user.id
    
    try:
        if call.data == "check_subscription":
            # Проверяем только каналы (реальная проверка)
            if check_subscription(user_id):
                # Создаем пользователя, если его еще нет
                if not users_table.search(User.user_id == user_id):
                    users_table.insert({
                        'user_id': user_id,
                        'balance': 0,
                        'first_name': call.from_user.first_name,
                        'last_name': call.from_user.last_name if call.from_user.last_name else '',
                        'referrer_id': None,
                        'completed_tasks': [],
                        'registration_date': datetime.now().isoformat(),
                        'referrals': [],
                        'username': call.from_user.username or "NoUsername",
                        'frozen': False,
                        'used_promo_codes': [],
                        'referral_rewarded': False
                    })
                else:
                    # Награждаем реферера после успешной проверки подписки
                    reward_referrer(user_id)
                
                bot.edit_message_text("<b>Главное меню</b>", chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode='HTML', reply_markup=main_menu_markup())
            else:
                bot.answer_callback_query(call.id, "Вы не подписаны на все каналы!")
                show_subscription_message(call.message.chat.id, user_id)
            return
        
        # Если пользователь не прошел проверку подписки на каналы
        if not check_subscription(user_id):
            show_subscription_message(call.message.chat.id, user_id)
            return
        
        user_data = users_table.search(User.user_id == user_id)
        if not user_data:
            bot.answer_callback_query(call.id, "Ошибка! Пользователь не найден.")
            return
            
        user_data = user_data[0]
        balance = user_data['balance']
        full_name = f"{user_data['first_name']} {user_data['last_name']}".strip()
        all_tasks = tasks_table.all()
        completed_tasks = user_data.get('completed_tasks', [])
        available_tasks = [task for task in all_tasks if task.doc_id not in completed_tasks]
        settings = settings_table.all()[0]
        min_referrals = settings.get('min_referrals', 5)
        min_tasks = settings.get('min_tasks', 3)
        referral_reward = settings.get('referral_reward', 1)

        if call.data.startswith("withdraw_sent_") or call.data.startswith("withdraw_denied_"):
            if call.from_user.id != ADMIN_ID:
                return
            
            parts = call.data.split("_")
            action = parts[1]
            withdrawal_id = int(parts[2])
            public_msg_id = int(parts[3])
            
            withdrawal = withdrawals_table.search(Withdrawal.id == withdrawal_id)
            if not withdrawal:
                bot.answer_callback_query(call.id, "Заявка не найдена!")
                return
                
            withdrawal = withdrawal[0]
            status = "Звезды отправлены" if action == "sent" else "Отказано"
            
            withdrawals_table.update({'status': status}, Withdrawal.id == withdrawal_id)
            target_user_id = withdrawal['user_id']
            stars = withdrawal['stars']
            
            public_text = f"Заявка №{withdrawal_id}\nПользователь: @{withdrawal['username']} | ID: {target_user_id}\nКоличество: {stars} звезд ⭐️\nСтатус: {status}"
            try:
                bot.edit_message_text(chat_id=PUBLIC_CHANNEL_ID, message_id=public_msg_id, text=public_text)
            except:
                pass
            
            admin_text = f"Заявка №{withdrawal_id}\nПользователь: @{withdrawal['username']} | ID: {target_user_id}\nКоличество: {stars} звезд ⭐️\nСтатус: {status}"
            try:
                bot.edit_message_text(chat_id=ADMIN_CHANNEL_ID, message_id=call.message.message_id, text=admin_text)
            except:
                pass
            
            if action == "sent":
                bot.send_message(target_user_id, f"Заявка №{withdrawal_id} выполнена!")
            else:
                bot.send_message(target_user_id, f"Заявка №{withdrawal_id} отклонена.")
                user = users_table.search(User.user_id == target_user_id)
                if user and not user[0].get('frozen', False):
                    new_balance = user[0]['balance'] + stars
                    users_table.update({'balance': new_balance}, User.user_id == target_user_id)
            return

        if call.data == "profile":
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Назад", callback_data="cancel"))
            reg_date = datetime.fromisoformat(user_data['registration_date']).strftime("%m.%d.%Y")
            referrals_count = len(user_data.get('referrals', []))
            tasks_count = len(completed_tasks)
            withdrawals_count = len([w for w in withdrawals_table.search(Withdrawal.user_id == user_id) if w.get('status') == 'Звезды отправлены'])
            profile_text = (
                "<b>Мой профиль</b>\n\n"
                f"ID: {user_id}\n"
                f"Регистрация: {reg_date}\n\n"
                f"Баланс: {balance} ⭐️\n"
                f"Выполненных заданий: {tasks_count}\n"
                f"Рефералов: {referrals_count}\n\n"
                "<b>Статистика</b>\n"
                f"Всего выводов: {withdrawals_count}"
            )
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=profile_text, reply_markup=markup, parse_mode='HTML')

        elif call.data == "referral":
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Назад", callback_data="cancel"))
            referrals_count = len(user_data.get('referrals', []))
            total_earned = referrals_count * referral_reward
            referral_link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
            referral_text = (
                "<b>Реферальная программа</b>\n\n"
                f"Приводи друзей и получай {referral_reward} ⭐️\n\n"
                "<b>⬇️ Ваша реферальная ссылка:</b>\n"
                f"{referral_link}\n\n"
                "<b>🥇 Статистика:</b>\n"
                f"├ Приглашено: {referrals_count}\n"
                f"└ Всего заработано: {total_earned} ⭐️"
            )
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=referral_text, reply_markup=markup, parse_mode='HTML')

        elif call.data.startswith("tasks_"):
            page = int(call.data.split("_")[1])
            if not available_tasks:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("Назад", callback_data="cancel"))
                bot.edit_message_text(
                    chat_id=call.message.chat.id,
                    message_id=call.message.message_id,
                    text="❗ Заданий пока нету.\n\n📅 Проверяй эту вкладку каждый час на наличие новых заданий!",
                    reply_markup=markup
                )
                return
            
            task = available_tasks[page - 1]
            markup = types.InlineKeyboardMarkup()
            btn_prev = types.InlineKeyboardButton("<", callback_data=f"tasks_{page-1}" if page > 1 else "none")
            btn_page = types.InlineKeyboardButton(f"{page}/{len(available_tasks)}", callback_data="none")
            btn_next = types.InlineKeyboardButton(">", callback_data=f"tasks_{page+1}" if page < len(available_tasks) else "none")
            markup.row(btn_prev, btn_page, btn_next)
            markup.row(types.InlineKeyboardButton("Проверить", callback_data=f"check_{task.doc_id}"))
            markup.row(types.InlineKeyboardButton("Назад", callback_data="cancel"))
            task_text = f"<b>Новое задание</b>\n\nСсылка: {task['link']}\nНаграда: {task['reward']} ⭐️"
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=task_text, reply_markup=markup, parse_mode='HTML')

        elif call.data.startswith("check_"):
            task_id = int(call.data.split("_")[1])
            task = tasks_table.get(doc_id=task_id)
            
            if not task:
                bot.answer_callback_query(call.id, "Задание не найдено!")
                return
            
            if task_id in completed_tasks:
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Ты уже выполнил это задание!")
                return
            
            if user_data.get('frozen', False):
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Твой аккаунт заморожен, выполнение заданий невозможно!")
                return
            
            try:
                chat_member = bot.get_chat_member(task['channel_id'], user_id)
                if chat_member.status in ['member', 'administrator', 'creator']:
                    new_balance = balance + task['reward']
                    completed_tasks.append(task_id)
                    users_table.update({'balance': new_balance, 'completed_tasks': completed_tasks}, User.user_id == user_id)
                    bot.send_message(chat_id=call.message.chat.id, text=f"Задание выполнено!\nТы получил: {task['reward']} звезд ⭐️\nНовый баланс: {new_balance} звезд ⭐️")
                    
                    available_tasks = [t for t in tasks_table.all() if t.doc_id not in completed_tasks]
                    if available_tasks:
                        next_task = available_tasks[0]
                        page = 1
                        markup = types.InlineKeyboardMarkup()
                        btn_prev = types.InlineKeyboardButton("<", callback_data="none")
                        btn_page = types.InlineKeyboardButton(f"1/{len(available_tasks)}", callback_data="none")
                        btn_next = types.InlineKeyboardButton(">", callback_data="tasks_2" if len(available_tasks) > 1 else "none")
                        markup.row(btn_prev, btn_page, btn_next)
                        markup.row(types.InlineKeyboardButton("Проверить", callback_data=f"check_{next_task.doc_id}"))
                        markup.row(types.InlineKeyboardButton("Назад", callback_data="cancel"))
                        next_task_text = f"<b>Новое задание</b>\n\nСсылка: {next_task['link']}\nНаграда: {next_task['reward']} ⭐️"
                        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=next_task_text, reply_markup=markup, parse_mode='HTML')
                    else:
                        markup = types.InlineKeyboardMarkup()
                        markup.add(types.InlineKeyboardButton("Назад", callback_data="cancel"))
                        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="❗ Заданий пока нету.\n\n📅 Проверяй эту вкладку каждый час на наличие новых заданий!", reply_markup=markup)
                else:
                    bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Ты не подписан на канал!\nПодпишись и попробуй снова.")
            except Exception as e:
                logger.error(f"Ошибка проверки подписки на канал задания: {e}")
                bot.answer_callback_query(call.id, "Ошибка при проверке подписки!")

        elif call.data == "promo":
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Введите промокод для активации:", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("Назад", callback_data="cancel")))

        elif call.data == "withdraw":
            referrals_count = len(user_data.get('referrals', []))
            tasks_count = len(completed_tasks)
            markup = types.InlineKeyboardMarkup()
            btn_15 = types.InlineKeyboardButton("15 звезд", callback_data="withdraw_15")
            btn_25 = types.InlineKeyboardButton("25 звезд", callback_data="withdraw_25")
            btn_50 = types.InlineKeyboardButton("50 звезд", callback_data="withdraw_50")
            btn_100 = types.InlineKeyboardButton("100 звезд", callback_data="withdraw_100")
            btn_150 = types.InlineKeyboardButton("150 звезд", callback_data="withdraw_150")
            btn_350 = types.InlineKeyboardButton("350 звезд", callback_data="withdraw_350")
            btn_500 = types.InlineKeyboardButton("500 звезд", callback_data="withdraw_500")
            btn_back = types.InlineKeyboardButton("Назад", callback_data="cancel")
            markup.row(btn_15, btn_25)
            markup.row(btn_50, btn_100)
            markup.row(btn_150, btn_350)
            markup.row(btn_500)
            markup.add(btn_back)
            withdraw_text = (
                f"<b>Вывод звезд</b>\n\n"
                f"Твой баланс: {balance} ⭐️\n"
                f"Нужно: {min_referrals} друзей и {min_tasks} заданий\n"
                f"У тебя: {referrals_count} друзей и {tasks_count} заданий\n\n"
                f"Выбери сумму для вывода:"
            )
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=withdraw_text, reply_markup=markup, parse_mode='HTML')

        elif call.data.startswith("withdraw_"):
            try:
                stars = int(call.data.split("_")[1])
            except:
                stars = 0
                
            referrals_count = len(user_data.get('referrals', []))
            tasks_count = len(completed_tasks)
            
            if user_data.get('frozen', False):
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Твой аккаунт заморожен, вывод невозможен!")
                return
            
            if balance < stars:
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=f"Недостаточно звезд!\nТвой баланс: {balance} звезд ⭐️")
                return
            if referrals_count < min_referrals or tasks_count < min_tasks:
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=f"Не хватает условий!\nНужно: {min_referrals} друзей и {min_tasks} заданий\nУ тебя: {referrals_count} друзей и {tasks_count} заданий")
                return
            
            withdrawal_id = len(withdrawals_table.all()) + 1
            withdrawals_table.insert({
                'id': withdrawal_id,
                'user_id': user_id,
                'stars': stars,
                'status': 'Ожидание',
                'username': user_data['username'],
                'timestamp': datetime.now().isoformat()
            })
            new_balance = balance - stars
            users_table.update({'balance': new_balance}, User.user_id == user_id)
            
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=f"Заявка №{withdrawal_id} на {stars} звезд ⭐️ создана!")
            
            public_text = f"Заявка №{withdrawal_id}\nПользователь: @{user_data['username']} | ID: {user_id}\nКоличество: {stars} звезд ⭐️\nСтатус: Ожидание"
            try:
                public_msg = bot.send_message(PUBLIC_CHANNEL_ID, public_text)
            except Exception as e:
                logger.error(f"Ошибка отправки в публичный канал: {e}")
                public_msg = type('obj', (object,), {'message_id': 0})
            
            admin_text = f"Заявка №{withdrawal_id}\nПользователь: @{user_data['username']} | ID: {user_id}\nКоличество: {stars} звезд ⭐️\nСтатус: Ожидание"
            markup = types.InlineKeyboardMarkup()
            btn_sent = types.InlineKeyboardButton("Отправлено", callback_data=f"withdraw_sent_{withdrawal_id}_{public_msg.message_id}")
            btn_denied = types.InlineKeyboardButton("Отказано", callback_data=f"withdraw_denied_{withdrawal_id}_{public_msg.message_id}")
            markup.add(btn_sent, btn_denied)
            try:
                admin_msg = bot.send_message(ADMIN_CHANNEL_ID, admin_text, reply_markup=markup)
                withdrawals_table.update({'admin_msg_id': admin_msg.message_id}, Withdrawal.id == withdrawal_id)
            except Exception as e:
                logger.error(f"Ошибка отправки в админ канал: {e}")

        elif call.data.startswith("top_"):
            period = call.data.split("_")[1]
            now = datetime.now()
            if period == "day":
                time_delta = timedelta(days=1)
                title = "Топ-5 за сутки"
                other1, other2 = "week", "month"
            elif period == "week":
                time_delta = timedelta(weeks=1)
                title = "Топ-5 за неделю"
                other1, other2 = "day", "month"
            else:
                time_delta = timedelta(days=30)
                title = "Топ-5 за месяц"
                other1, other2 = "day", "week"

            all_users = users_table.all()
            top_users = []
            for user in all_users:
                referrals = user.get('referrals', [])
                recent_referrals = []
                for r in referrals:
                    try:
                        ref_user = users_table.search(User.user_id == r)
                        if ref_user:
                            reg_date = datetime.fromisoformat(ref_user[0]['registration_date'])
                            if now - reg_date <= time_delta:
                                recent_referrals.append(r)
                    except:
                        continue
                if recent_referrals:
                    top_users.append((f"{user['first_name']} {user['last_name']}".strip(), len(recent_referrals), user['user_id']))

            top_users.sort(key=lambda x: x[1], reverse=True)
            top_5 = top_users[:5]
            
            top_text = f"<b>{title}</b>\n\n"
            if top_5:
                for i, (name, ref, _) in enumerate(top_5):
                    top_text += f"{i+1}. {name} - {ref} друзей\n"
            else:
                top_text += "Пока нет данных для топа!"
            
            markup = types.InlineKeyboardMarkup()
            btn_other1 = types.InlineKeyboardButton(f"За {'сутки' if other1 == 'day' else 'неделю' if other1 == 'week' else 'месяц'}", callback_data=f"top_{other1}")
            btn_other2 = types.InlineKeyboardButton(f"За {'сутки' if other2 == 'day' else 'неделю' if other2 == 'week' else 'месяц'}", callback_data=f"top_{other2}")
            btn_back = types.InlineKeyboardButton("Назад", callback_data="cancel")
            markup.row(btn_other1, btn_other2)
            markup.add(btn_back)
            
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=top_text, reply_markup=markup, parse_mode='HTML')

        elif call.data == "admin_add":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'task_channel_id'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите ID канала (например, @ChannelName или -100123456789):", reply_markup=markup)

        elif call.data == "admin_delete":
            if user_id != ADMIN_ID:
                return
            all_tasks = tasks_table.all()
            if not all_tasks:
                bot.send_message(call.message.chat.id, "Заданий нет!")
                return
            tasks_list = "\n".join([f"{i+1}. {t['link']} - {t['reward']} звезд ⭐️" for i, t in enumerate(all_tasks)])
            admin_commands_table.insert({'user_id': user_id, 'step': 'delete_task'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, f"Список заданий:\n{tasks_list}\n\nВведи номер задания для удаления:", reply_markup=markup)

        elif call.data == "admin_add_channel":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'channel_id'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите ID канала (например, @ChannelName или -100123456789):", reply_markup=markup)

        elif call.data == "admin_delete_channel":
            if user_id != ADMIN_ID:
                return
            all_channels = channels_table.all()
            if not all_channels:
                bot.send_message(call.message.chat.id, "Каналов нет!")
                return
            channels_list = "\n".join([f"{i+1}. {c['link']}" for i, c in enumerate(all_channels)])
            admin_commands_table.insert({'user_id': user_id, 'step': 'delete_channel'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, f"Список каналов:\n{channels_list}\n\nВведи номер канала для удаления:", reply_markup=markup)

        elif call.data == "admin_add_bot":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'bot_username'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите username бота (например, @MyBot):", reply_markup=markup)

        elif call.data == "admin_delete_bot":
            if user_id != ADMIN_ID:
                return
            all_bots = bots_table.all()
            if not all_bots:
                bot.send_message(call.message.chat.id, "Ботов нет!")
                return
            bots_list = "\n".join([f"{i+1}. {b['name']} - @{b['username']}" for i, b in enumerate(all_bots)])
            admin_commands_table.insert({'user_id': user_id, 'step': 'delete_bot'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, f"Список ботов:\n{bots_list}\n\nВведи номер бота для удаления:", reply_markup=markup)

        elif call.data == "admin_subscription_stats":
            if user_id != ADMIN_ID:
                return
            
            channels = channels_table.all()
            bots = bots_table.all()
            
            stats_text = "<b>Статистика подписок</b>\n\n"
            
            # Статистика по каналам
            if channels:
                stats_text += "<b>Каналы:</b>\n"
                for channel in channels:
                    try:
                        subscribers_count = bot.get_chat_members_count(channel['channel_id'])
                        stats_text += f"• {channel.get('name', 'Канал')}: {subscribers_count} подписчиков\n"
                    except Exception as e:
                        stats_text += f"• {channel.get('name', 'Канал')}: ошибка получения данных\n"
            
            # Статистика по ботам
            if bots:
                stats_text += "\n<b>Боты:</b>\n"
                for bot_data in bots:
                    stats_text += f"• {bot_data.get('name', 'Бот')}: @{bot_data['username']}\n"
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_back"))
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=stats_text, reply_markup=markup, parse_mode='HTML')

        elif call.data == "admin_set_min_refs":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'set_min_refs'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите минимальное количество рефералов:", reply_markup=markup)

        elif call.data == "admin_set_min_tasks":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'set_min_tasks'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите минимальное количество заданий:", reply_markup=markup)

        elif call.data == "admin_set_ref_reward":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'set_ref_reward'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена",callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите количество звезд за одного реферала:", reply_markup=markup)

        elif call.data == "admin_add_promo":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'promo_code'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите промокод что нужно будет вводить:", reply_markup=markup)

        elif call.data == "admin_delete_promo":
            if user_id != ADMIN_ID:
                return
            all_promos = promo_table.all()
            if not all_promos:
                bot.send_message(call.message.chat.id, "Промокодов нет!")
                return
            promo_list = "\n".join([f"{i+1}. {p['code']} - {p['reward']} звезд ⭐️ (Лимит: {p['limit']}, Использовано: {len(p.get('used_by', []))})" for i, p in enumerate(all_promos)])
            admin_commands_table.insert({'user_id': user_id, 'step': 'delete_promo'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, f"Список промокодов:\n{promo_list}\n\nВведите номер промокода который хотите удалить:", reply_markup=markup)

        elif call.data == "admin_stats":
            if user_id != ADMIN_ID:
                return
            now = datetime.now()
            users = users_table.all()
            withdrawals = withdrawals_table.all()
            
            users_day = len([u for u in users if (now - datetime.fromisoformat(u['registration_date'])).days <= 1])
            users_week = len([u for u in users if (now - datetime.fromisoformat(u['registration_date'])).days <= 7])
            users_month = len([u for u in users if (now - datetime.fromisoformat(u['registration_date'])).days <= 30])
            users_total = len(users)
            
            stars_day = sum(w['stars'] for w in withdrawals if w.get('status') == 'Звезды отправлены' and (now - datetime.fromisoformat(w['timestamp'])).days <= 1)
            stars_week = sum(w['stars'] for w in withdrawals if w.get('status') == 'Звезды отправлены' and (now - datetime.fromisoformat(w['timestamp'])).days <= 7)
            stars_month = sum(w['stars'] for w in withdrawals if w.get('status') == 'Звезды отправлены' and (now - datetime.fromisoformat(w['timestamp'])).days <= 30)
            stars_total = sum(w['stars'] for w in withdrawals if w.get('status') == 'Звезды отправлены')
            
            stats_text = (
                "<b>Статистика бота</b>\n\n"
                "<b>Пользователи:</b>\n"
                f"За сутки: {users_day}\n"
                f"За неделю: {users_week}\n"
                f"За месяц: {users_month}\n"
                f"За всё время: {users_total}\n\n"
                "<b>Выведено звезд:</b>\n"
                f"За сутки: {stars_day} ⭐️\n"
                f"За неделю: {stars_week} ⭐️\n"
                f"За месяц: {stars_month} ⭐️\n"
                f"За всё время: {stars_total} ⭐️"
            )
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_back"))
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=stats_text, reply_markup=markup, parse_mode='HTML')

        elif call.data.startswith("admin_task_stats_"):
            if user_id != ADMIN_ID:
                return
            page = int(call.data.split("_")[3])
            all_tasks = tasks_table.all()
            if not all_tasks:
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_back"))
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Заданий нет!", reply_markup=markup)
                return
            
            task = all_tasks[page - 1]
            now = datetime.now()
            completed_day = 0
            completed_week = 0
            completed_month = 0
            completed_total = 0
            
            for user in users_table.all():
                completed_tasks = user.get('completed_tasks', [])
                if task.doc_id in completed_tasks:
                    reg_date = datetime.fromisoformat(user['registration_date'])
                    days_diff = (now - reg_date).days
                    
                    completed_total += 1
                    if days_diff <= 30:
                        completed_month += 1
                        if days_diff <= 7:
                            completed_week += 1
                            if days_diff <= 1:
                                completed_day += 1
            
            task_text = (
                f"Ссылка: {task['link']}\n"
                f"Награда: {task['reward']} ⭐️\n\n"
                "<b>Статистика</b>\n"
                f"За сегодня: {completed_day}\n"
                f"За неделю: {completed_week}\n"
                f"За месяц: {completed_month}\n"
                f"За всё время: {completed_total}"
            )
            markup = types.InlineKeyboardMarkup()
            btn_prev = types.InlineKeyboardButton("⬅️", callback_data=f"admin_task_stats_{page-1}" if page > 1 else "none")
            btn_page = types.InlineKeyboardButton(f"{page}/{len(all_tasks)}", callback_data="none")
            btn_next = types.InlineKeyboardButton("➡️", callback_data=f"admin_task_stats_{page+1}" if page < len(all_tasks) else "none")
            btn_delete = types.InlineKeyboardButton("Удалить задание", callback_data=f"admin_delete_task_{task.doc_id}")
            btn_back = types.InlineKeyboardButton("Назад", callback_data="admin_back")
            markup.row(btn_prev, btn_page, btn_next)
            markup.add(btn_delete)
            markup.add(btn_back)
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=task_text, reply_markup=markup, parse_mode='HTML')

        elif call.data.startswith("admin_delete_task_"):
            if user_id != ADMIN_ID:
                return
            task_id = int(call.data.split("_")[3])
            tasks_table.remove(doc_ids=[task_id])
            bot.answer_callback_query(call.id, "Задание удалено!")
            all_tasks = tasks_table.all()
            page = 1
            if all_tasks:
                task = all_tasks[0]
                completed_day = 0
                completed_week = 0
                completed_month = 0
                completed_total = 0
                
                for user in users_table.all():
                    completed_tasks = user.get('completed_tasks', [])
                    if task.doc_id in completed_tasks:
                        reg_date = datetime.fromisoformat(user['registration_date'])
                        days_diff = (datetime.now() - reg_date).days
                        
                        completed_total += 1
                        if days_diff <= 30:
                            completed_month += 1
                            if days_diff <= 7:
                                completed_week += 1
                                if days_diff <= 1:
                                    completed_day += 1
                
                task_text = (
                    f"Ссылка: {task['link']}\n"
                    f"Награда: {task['reward']} ⭐️\n\n"
                    "<b>Статистика</b>\n"
                    f"За сегодня: {completed_day}\n"
                    f"За неделю: {completed_week}\n"
                    f"За месяц: {completed_month}\n"
                    f"За всё время: {completed_total}"
                )
                markup = types.InlineKeyboardMarkup()
                btn_prev = types.InlineKeyboardButton("⬅️", callback_data="none")
                btn_page = types.InlineKeyboardButton(f"1/{len(all_tasks)}", callback_data="none")
                btn_next = types.InlineKeyboardButton("➡️", callback_data="admin_task_stats_2" if len(all_tasks) > 1 else "none")
                btn_delete = types.InlineKeyboardButton("Удалить задание", callback_data=f"admin_delete_task_{all_tasks[0].doc_id}")
                btn_back = types.InlineKeyboardButton("Назад", callback_data="admin_back")
                markup.row(btn_prev, btn_page, btn_next)
                markup.add(btn_delete)
                markup.add(btn_back)
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=task_text, reply_markup=markup, parse_mode='HTML')
            else:
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Заданий нет!", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("Назад", callback_data="admin_back")))

        elif call.data.startswith("admin_users_"):
            if user_id != ADMIN_ID:
                return
            page = int(call.data.split("_")[2])
            all_users = users_table.all()
            if not all_users:
                bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="Пользователей нет!", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("Назад", callback_data="admin_back")))
                return
            
            user = all_users[page - 1]
            reg_date = datetime.fromisoformat(user['registration_date']).strftime("%m.%d.%Y")
            status = "Заморожен" if user.get('frozen', False) else "Рабочий"
            user_text = (
                f"ID: {user['user_id']}\n"
                f"Username: @{user['username']}\n\n"
                f"Баланс: {user['balance']} ⭐️\n"
                f"Выполненных заданий: {len(user.get('completed_tasks', []))}\n"
                f"Рефералов: {len(user.get('referrals', []))}\n\n"
                f"Регистрация: {reg_date}\n"
                f"Статус: {status}"
            )
            markup = types.InlineKeyboardMarkup()
            btn_prev = types.InlineKeyboardButton("⬅️", callback_data=f"admin_users_{page-1}" if page > 1 else "none")
            btn_page = types.InlineKeyboardButton(f"{page}/{len(all_users)}", callback_data="none")
            btn_next = types.InlineKeyboardButton("➡️", callback_data=f"admin_users_{page+1}" if page < len(all_users) else "none")
            btn_freeze = types.InlineKeyboardButton("Заморозить аккаунт", callback_data=f"freeze_{user['user_id']}")
            btn_unfreeze = types.InlineKeyboardButton("Разморозить аккаунт", callback_data=f"unfreeze_{user['user_id']}")
            btn_reset = types.InlineKeyboardButton("Обнулить аккаунт", callback_data=f"reset_{user['user_id']}")
            btn_back = types.InlineKeyboardButton("Назад", callback_data="admin_back")
            markup.row(btn_prev, btn_page, btn_next)
            markup.add(btn_freeze)
            markup.add(btn_unfreeze)
            markup.add(btn_reset)
            markup.add(btn_back)
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text=user_text, reply_markup=markup)

        elif call.data.startswith("freeze_"):
            if user_id != ADMIN_ID:
                return
            target_id = int(call.data.split("_")[1])
            users_table.update({'frozen': True}, User.user_id == target_id)
            bot.answer_callback_query(call.id, f"Пользователь {target_id} заморожен!")

        elif call.data.startswith("unfreeze_"):
            if user_id != ADMIN_ID:
                return
            target_id = int(call.data.split("_")[1])
            users_table.update({'frozen': False}, User.user_id == target_id)
            bot.answer_callback_query(call.id, f"Пользователь {target_id} разморожен!")

        elif call.data.startswith("reset_"):
            if user_id != ADMIN_ID:
                return
            target_id = int(call.data.split("_")[1])
            users_table.update({
                'balance': 0,
                'completed_tasks': [],
                'referrals': [],
                'used_promo_codes': [],
                'frozen': False,
                'referral_rewarded': False
            }, User.user_id == target_id)
            bot.answer_callback_query(call.id, f"Аккаунт пользователя {target_id} обнулен!")

        elif call.data == "admin_freeze":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'freeze'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите ID пользователя которого хотите заморозить:", reply_markup=markup)

        elif call.data == "admin_unfreeze":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'unfreeze'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите ID пользователя которого хотите разморозить:", reply_markup=markup)

        elif call.data == "admin_reset":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'reset'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите ID пользователя которого хотите обнулить аккаунт:", reply_markup=markup)

        elif call.data == "admin_broadcast":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.insert({'user_id': user_id, 'step': 'broadcast'})
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("Отмена", callback_data="admin_cancel"))
            bot.send_message(call.message.chat.id, "Введите сообщение для рассылки:", reply_markup=markup)

        elif call.data == "admin_cancel":
            if user_id != ADMIN_ID:
                return
            admin_commands_table.remove(AdminCommand.user_id == user_id)
            bot.answer_callback_query(call.id, "Действие отменено!")
            markup = types.InlineKeyboardMarkup()
            btn_stats = types.InlineKeyboardButton("Статистика", callback_data="admin_stats")
            btn_task_stats = types.InlineKeyboardButton("Статистика заданий", callback_data="admin_task_stats_1")
            btn_users = types.InlineKeyboardButton("Пользователи", callback_data="admin_users_1")
            btn_add_task = types.InlineKeyboardButton("Добавить задание", callback_data="admin_add")
            btn_delete_task = types.InlineKeyboardButton("Удалить задание", callback_data="admin_delete")
            btn_add_channel = types.InlineKeyboardButton("Добавить канал", callback_data="admin_add_channel")
            btn_delete_channel = types.InlineKeyboardButton("Удалить канал", callback_data="admin_delete_channel")
            btn_add_bot = types.InlineKeyboardButton("Добавить бота", callback_data="admin_add_bot")
            btn_delete_bot = types.InlineKeyboardButton("Удалить бота", callback_data="admin_delete_bot")
            btn_subscription_stats = types.InlineKeyboardButton("Статистика подписок", callback_data="admin_subscription_stats")
            btn_add_promo = types.InlineKeyboardButton("Создать промокод", callback_data="admin_add_promo")
            btn_delete_promo = types.InlineKeyboardButton("Удалить промокод", callback_data="admin_delete_promo")
            btn_set_min_refs = types.InlineKeyboardButton("Установить мин. рефералов", callback_data="admin_set_min_refs")
            btn_set_min_tasks = types.InlineKeyboardButton("Установить мин. заданий", callback_data="admin_set_min_tasks")
            btn_set_ref_reward = types.InlineKeyboardButton("Установить награду за реферала", callback_data="admin_set_ref_reward")
            btn_freeze = types.InlineKeyboardButton("Заморозить", callback_data="admin_freeze")
            btn_unfreeze = types.InlineKeyboardButton("Разморозить", callback_data="admin_unfreeze")
            btn_reset = types.InlineKeyboardButton("Обнулить аккаунт", callback_data="admin_reset")
            btn_broadcast = types.InlineKeyboardButton("Рассылка", callback_data="admin_broadcast")
            markup.add(btn_stats, btn_task_stats)
            markup.add(btn_users)
            markup.add(btn_add_task, btn_delete_task)
            markup.add(btn_add_channel, btn_delete_channel)
            markup.add(btn_add_bot, btn_delete_bot)
            markup.add(btn_subscription_stats)
            markup.add(btn_add_promo, btn_delete_promo)
            markup.add(btn_set_min_refs, btn_set_min_tasks)
            markup.add(btn_set_ref_reward)
            markup.add(btn_freeze, btn_unfreeze)
            markup.add(btn_reset)
            markup.add(btn_broadcast)
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="<b>Админ-панель</b>", reply_markup=markup, parse_mode='HTML')

        elif call.data == "admin_back":
            if user_id != ADMIN_ID:
                return
            markup = types.InlineKeyboardMarkup()
            btn_stats = types.InlineKeyboardButton("Статистика", callback_data="admin_stats")
            btn_task_stats = types.InlineKeyboardButton("Статистика заданий", callback_data="admin_task_stats_1")
            btn_users = types.InlineKeyboardButton("Пользователи", callback_data="admin_users_1")
            btn_add_task = types.InlineKeyboardButton("Добавить задание", callback_data="admin_add")
            btn_delete_task = types.InlineKeyboardButton("Удалить задание", callback_data="admin_delete")
            btn_add_channel = types.InlineKeyboardButton("Добавить канал", callback_data="admin_add_channel")
            btn_delete_channel = types.InlineKeyboardButton("Удалить канал", callback_data="admin_delete_channel")
            btn_add_bot = types.InlineKeyboardButton("Добавить бота", callback_data="admin_add_bot")
            btn_delete_bot = types.InlineKeyboardButton("Удалить бота", callback_data="admin_delete_bot")
            btn_subscription_stats = types.InlineKeyboardButton("Статистика подписок", callback_data="admin_subscription_stats")
            btn_add_promo = types.InlineKeyboardButton("Создать промокод", callback_data="admin_add_promo")
            btn_delete_promo = types.InlineKeyboardButton("Удалить промокод", callback_data="admin_delete_promo")
            btn_set_min_refs = types.InlineKeyboardButton("Установить мин. рефералов", callback_data="admin_set_min_refs")
            btn_set_min_tasks = types.InlineKeyboardButton("Установить мин. заданий", callback_data="admin_set_min_tasks")
            btn_set_ref_reward = types.InlineKeyboardButton("Установить награду за реферала", callback_data="admin_set_ref_reward")
            btn_freeze = types.InlineKeyboardButton("Заморозить", callback_data="admin_freeze")
            btn_unfreeze = types.InlineKeyboardButton("Разморозить", callback_data="admin_unfreeze")
            btn_reset = types.InlineKeyboardButton("Обнулить аккаунт", callback_data="admin_reset")
            btn_broadcast = types.InlineKeyboardButton("Рассылка", callback_data="admin_broadcast")
            markup.add(btn_stats, btn_task_stats)
            markup.add(btn_users)
            markup.add(btn_add_task, btn_delete_task)
            markup.add(btn_add_channel, btn_delete_channel)
            markup.add(btn_add_bot, btn_delete_bot)
            markup.add(btn_subscription_stats)
            markup.add(btn_add_promo, btn_delete_promo)
            markup.add(btn_set_min_refs, btn_set_min_tasks)
            markup.add(btn_set_ref_reward)
            markup.add(btn_freeze, btn_unfreeze)
            markup.add(btn_reset)
            markup.add(btn_broadcast)
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="<b>Админ-панель</b>", reply_markup=markup, parse_mode='HTML')

        elif call.data == "cancel":
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="<b>Главное меню</b>", reply_markup=main_menu_markup(), parse_mode='HTML')
            
        elif call.data == "none":
            bot.answer_callback_query(call.id, "")
            
    except Exception as e:
        logger.error(f"Ошибка в callback_handler: {e}")
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка!")
        except:
            pass

# ==============================================
# ФУНКЦИЯ ДЛЯ РУЧНОГО ВОССТАНОВЛЕНИЯ
# ==============================================

def manual_recovery():
    """Функция для ручного запуска восстановления"""
    logger.info("🔄 Запуск ручного восстановления...")
    db_path_local = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'baseоповарп2n.json')
    
    # Восстанавливаем данные
    recovered_data = deep_recover_database(db_path_local)
    
    if recovered_data['users']:
        logger.info(f"✅ Восстановлено {len(recovered_data['users'])} пользователей")
        
        # Создаем отдельный файл с восстановленными данными
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        recovered_file = f"recovered_users_{timestamp}.json"
        
        with open(recovered_file, 'w', encoding='utf-8') as f:
            json.dump(recovered_data['users'], f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Список восстановленных пользователей сохранен в: {recovered_file}")
        
        # Показываем всех пользователей
        logger.info("📋 Все восстановленные пользователи:")
        for i, user in enumerate(recovered_data['users']):
            logger.info(f"   {i+1}. ID: {user['user_id']}, Имя: {user['first_name']}, Баланс: {user['balance']}, Username: {user['username']}")
    
    return recovered_data

# ==============================================
# ЗАПУСК БОТА
# ==============================================

if __name__ == "__main__":
    logger.info("=== БОТ С ГЛУБОКИМ ВОССТАНОВЛЕНИЕМ ЗАПУЩЕН ===")
    logger.info(f"📁 База данных: {db_path}")
    
    # Проверяем количество восстановленных пользователей
    try:
        users_count = len(users_table.all())
        logger.info(f"👥 Пользователей в базе: {users_count}")
        
        if users_count < 30:  # Если мало пользователей
            logger.warning(f"⚠️ Обнаружено мало пользователей ({users_count}). Попробуем ручное восстановление...")
            recovered_data = manual_recovery()
            
            if len(recovered_data['users']) > users_count:
                logger.info(f"🎯 Ручное восстановление нашло {len(recovered_data['users'])} пользователей")
                logger.info("🔄 Создаем новую базу с восстановленными данными...")
                
                # Создаем новую базу
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                new_db_file = f"new_database_{timestamp}.json"
                create_deep_recovered_database(new_db_file, recovered_data)
                
                logger.info(f"✅ Новая база создана: {new_db_file}")
                logger.info(f"⚠️ Замените файл {db_path} на {new_db_file} для использования восстановленной базы")
        
        # Показываем информацию о пользователях
        if users_count > 0:
            logger.info("📋 Список пользователей в текущей базе:")
            for i, user in enumerate(users_table.all()[:30]):
                logger.info(f"   {i+1}. ID: {user.get('user_id')}, Имя: {user.get('first_name')}, Баланс: {user.get('balance')} ⭐️")
            
            if users_count > 30:
                logger.info(f"   ... и еще {users_count - 30} пользователей")
    except Exception as e:
        logger.error(f"❌ Ошибка проверки базы: {e}")
    
    # Основной цикл бота
    while True:
        try:
            bot.polling(none_stop=True, interval=1, timeout=30)
        except Exception as e:
            logger.error(f"❌ Ошибка polling: {e}")
            time.sleep(5)
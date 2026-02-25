import asyncio
import faust
import json
import logging
import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Check if event loop exists, create one if not
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    # No running loop, create a new one
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# Fix for aiokafka compatibility issue
import faust.transport.drivers.aiokafka as aiokafka_driver
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

def patched_new_producer(self, **kwargs):
    # Remove api_version if present
    if 'api_version' in kwargs:
        del kwargs['api_version']
    # Create producer directly without api_version
    return AIOKafkaProducer(**kwargs)

# Patch the method that creates producer
aiokafka_driver.Producer._new_producer = patched_new_producer

# Patch AIOKafkaConsumer constructor to remove api_version parameter
original_init = AIOKafkaConsumer.__init__

def patched_consumer_init(self, *args, **kwargs):
    # Remove api_version if present
    if 'api_version' in kwargs:
        del kwargs['api_version']
    return original_init(self, *args, **kwargs)

AIOKafkaConsumer.__init__ = patched_consumer_init

from messages.example_message import MyMessage
from messages.service_messages import BlockEvent, CensorWordsUpdate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Kafka брокеры
BOOTSTRAP_SERVERS = [
    'localhost:9092',
    'localhost:9093',
    'localhost:9094'
]

# Настройки Faust
FAUST_APP_NAME: str = 'kafka-censorship-system'
FAUST_BROKER: str = ','.join(BOOTSTRAP_SERVERS)
FAUST_STORE: str = 'memory://'

DEFAULT_CENSORED_WORDS = [
                'spam', 'advertisement', 'promo', 'discount',
                'badword', 'offensive', 'hate', 'violence',
                'sex', 'porn', 'gambling', 'casino'
            ]

# Создаем Faust приложение
app = faust.App(
    id=FAUST_APP_NAME,
    broker=FAUST_BROKER,
    store=FAUST_STORE
)

# Определяем топики
messages_topic = app.topic(
    'messages',
    value_type=MyMessage,
    partitions=3,
    retention=24 * 60 * 60  # 24 часа
)

# Отфильтрованные и обработанные сообщения
filtered_messages_topic = app.topic(
    'filtered_messages',
    value_type=MyMessage,
    partitions=3
)

blocked_users_topic = app.topic(
    'blocked_users',
    value_type=BlockEvent,
    partitions=3
)

censored_words_topic = app.topic(
    'censored_words',
    value_type=CensorWordsUpdate,
    partitions=1  # Одна партиция для простоты
)

# Таблица 1: Хранение списков заблокированных пользователей
blocked_users_table = app.Table(
    'blocked_users',
    default=list,
    partitions=3,
    help='Хранит списки заблокированных пользователей для каждого получателя'
)

# Таблица 2: Хранение списка запрещенных слов
censored_words_table = app.Table(
    'censored_words',
    default=lambda: DEFAULT_CENSORED_WORDS,
    partitions=1,
    help='Хранит глобальный список запрещенных слов'
)

# Поток обработки событий блокировки
@app.agent(blocked_users_topic)
async def process_block_events(stream):
    # Обработка событий блокировки пользователей
    async for event in stream:
        logger.info(f"Processing block event: {event.user_id} -> {event.action} {event.blocked_user_id}")
        
        # Получаем текущий список или создаем новый
        current_blocked = list(blocked_users_table[event.user_id])
        
        if event.action == 'block':
            if event.blocked_user_id not in current_blocked:
                current_blocked.append(event.blocked_user_id)
                blocked_users_table[event.user_id] = current_blocked
        elif event.action == 'unblock':
            if event.blocked_user_id in current_blocked:
                current_blocked.remove(event.blocked_user_id)
                blocked_users_table[event.user_id] = current_blocked

# Поток обработки обновлений списка запрещенных слов
@app.agent(censored_words_topic)
async def process_censored_words(stream):
    # Обработка обновлений списка запрещенных слов
    async for update in stream:
        logger.info(f"Processing censored word update: {update.action} {update.word}")
        
        # Получаем текущий список или создаем новый
        current_words = list(censored_words_table['censored_words'])
        
        if update.action == 'add':
            if update.word not in current_words:
                current_words.append(update.word)
                censored_words_table['censored_words'] = current_words
        elif update.action == 'remove':
            if update.word in current_words:
                current_words.remove(update.word)
                censored_words_table['censored_words'] = current_words

def censor_text(text: str, censored_words: list) -> str:
    result = text
    for word in censored_words:
        if word.lower() in result.lower():
            result = result.replace(word, '*' * len(word))
            result = result.replace(word.capitalize(), '*' * len(word))
            result = result.replace(word.upper(), '*' * len(word))
    return result

# Поток обработки сообщений с цензурой и проверкой блокировки
@app.agent(messages_topic, sink=[filtered_messages_topic])
async def process_messages(stream):
    # Обработка сообщений с применением цензуры и проверкой блокировки
    async for message in stream:
        # Проверяем, заблокирован ли отправитель получателем
        blocked_list = list(blocked_users_table[message.recipient_id])
        
        if message.sender_id not in blocked_list:
            # Применяем цензуру
            censored_words = list(censored_words_table['censored_words'])
            censored_content = censor_text(message.content, censored_words)
            
            # Создаем цензурированное сообщение
            censored_message = MyMessage(
                message_id=message.message_id,
                sender_id=message.sender_id,
                recipient_id=message.recipient_id,
                content=censored_content
            )
            
            yield censored_message
        else:
            logger.info(f"Message from {message.sender_id} to {message.recipient_id} blocked - sender is in blocked list")


if __name__ == '__main__':
    app.main()

# Система обработки потоков сообщений с функциями блокировки пользователей и цензуры

## Описание проекта

Проект представляет собой систему потоковой обработки сообщений на базе Apache Kafka с использованием библиотеки Faust. Система обеспечивает:

- **Блокировку пользователей**: Пользователи могут блокировать других пользователей, сообщения от которых не будут доставляться
- **Цензуру сообщений**: Автоматическая фильтрация и маскировка запрещенных слов в сообщениях
- **Постоянное хранение**: Использование таблиц Faust для хранения списков блокировок и запрещенных слов

## Архитектура системы

### Компоненты

#### 1. Основные приложения

**streaming_app.py** - Основное потоковое приложение на Faust
- `process_block_events()` - Обработка событий блокировки/разблокировки пользователей
- `process_censored_words()` - Обработка обновлений списка запрещенных слов  
- `process_messages()` - Основная обработка сообщений с применением цензуры и фильтрации
- `censor_text()` - Функция цензуры текста сообщений

#### 2. Сообщения и данные

**messages/example_message.py** - Основная модель сообщения
```python
@dataclass
class MyMessage(faust.Record, serializer='json'):
    message_id: int
    sender_id: int  
    recipient_id: int
    content: str
```

**messages/service_messages.py** - Сервисные сообщения
- `BlockEvent` - События блокировки пользователей
- `CensorWordsUpdate` - Обновления списка запрещенных слов

#### 3. Продюсеры и консьюмеры

**producers/message_producer.py** - Генератор тестовых сообщений
- `MyProducer` - Класс для отправки сообщений в Kafka
- Включает тестовые сообщения с запрещенными словами

### Топики Kafka

- **messages** - Входящие сообщения для обработки
- **filtered_messages** - Обработанные сообщения после цензуры и фильтрации
- **blocked_users** - События блокировки/разблокировки пользователей
- **censored_words** - Обновления списка запрещенных слов

### Таблицы хранения

- **blocked_users_table** - Хранение списков заблокированных пользователей для каждого получателя
- **censored_words_table** - Глобальный список запрещенных слов

## Инструкция по запуску

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Запуск инфраструктуры Kafka

```bash
docker-compose up -d
```

### 3. Проверка контейнеров

```bash
docker-compose ps
```

### 4. Создание топиков

```bash
# Создание топика для сообщений
docker exec -it kafka_ya1 kafka-topics --create --topic messages --partitions 3 --replication-factor 1 --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094

# Создание топика для отфильтрованных сообщений  
docker exec -it kafka_ya1 kafka-topics --create --topic filtered_messages --partitions 3 --replication-factor 1 --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094

# Создание топика для событий блокировки
docker exec -it kafka_ya1 kafka-topics --create --topic blocked_users --partitions 3 --replication-factor 1 --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094

# Создание топика для обновления запрещенных слов
docker exec -it kafka_ya1 kafka-topics --create --topic censored_words --partitions 1 --replication-factor 1 --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094
```

### 5. Запуск приложений

**Основное потоковое приложение (Faust):**
```bash
python streaming_app.py
```

**Продюсер тестовых сообщений:**
```bash
python producers/message_producer.py 50
```

## Инструкция по тестированию

### Тестирование блокировки пользователей

1. **Запустите основное приложение:**
```bash
python streaming_app.py
```

2. **В другом терминале запустите продюсер для генерации сообщений:**
```bash
python producers/message_producer.py 20
```

3. **Отправьте событие блокировки:**
```bash
# Создайте файл block_event.json с содержимым:
{"user_id": "1", "blocked_user_id": "2", "action": "block", "timestamp": 1640995200.0}

# Отправьте в топик blocked_users:
docker exec -it kafka_ya1 kafka-console-producer --topic blocked_users --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 < block_event.json
```

4. **Ожидаемый результат:** Сообщения от пользователя 2 пользователю 1 больше не будут доставляться

### Тестирование цензуры сообщений

1. **Запустите приложение и продюсер (если еще не запущены)**

2. **Отправьте обновление списка запрещенных слов:**
```bash
# Создайте файл censor_update.json с содержимым:
{"word": "spam", "action": "add", "timestamp": 1640995200.0}

# Отправьте в топик censored_words:
docker exec -it kafka_ya1 kafka-console-producer --topic censored_words --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 < censor_update.json
```

3. **Ожидаемый результат:** Слово "spam" в сообщениях будет заменено на "****"

### Тестовые данные

**Тестовые сообщения с запрещенными словами:**
```python
contents = [
    'Hello, how are you today?',
    'I\'m fine, thanks!',
    'Check out this spam message!',
    'Amazing discount 50 off!!!',
    'This is offensive content',
    'Don\'t advertise here',
    'I hate this violence',
    'Promo code: DISCOUNT50',
    'Casino gambling is illegal here'
]
```

**События блокировки:**
```json
// Блокировка пользователя
{"user_id": "1", "blocked_user_id": "2", "action": "block", "timestamp": 1640995200.0}

// Разблокировка пользователя  
{"user_id": "1", "blocked_user_id": "2", "action": "unblock", "timestamp": 1640995200.0}
```

**Обновления списка запрещенных слов:**
```json
// Добавление слова
{"word": "spam", "action": "add", "timestamp": 1640995200.0}

// Удаление слова
{"word": "spam", "action": "remove", "timestamp": 1640995200.0}
```

## Мониторинг и отладка

### Kafka UI

Доступ к веб-интерфейсу Kafka UI: http://localhost:8280

- Просмотр топиков и сообщений
- Мониторинг консьюмеров
- Управление схемами

### Schema Registry

Доступ к Schema Registry: http://localhost:8081

### Логирование

Все компоненты используют стандартный модуль logging с уровнем INFO:
```python
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
```

## Критерии успешного выполнения

- ✅ Система блокировки пользователей работает корректно
- ✅ Цензура сообщений применяется автоматически  
- ✅ Данные сохраняются в таблицах Faust
- ✅ Все топики Kafka созданы и работают
- ✅ Docker-инфраструктура запускается без ошибок
- ✅ Тестовые данные обрабатываются корректно
- ✅ Система масштабируется с несколькими консьюмерами

## Возможные проблемы и решения

### Проблема: Соединение с Kafka не устанавливается
**Решение:** Убедитесь, что все контейнеры запущены:
```bash
docker-compose ps
docker-compose logs kafka1
```

### Проблема: Топики не создаются
**Решение:** Проверьте доступность Kafka брокеров:
```bash
docker exec -it kafka_ya1 kafka-topics --list --bootstrap-server kafka1:29092
```

### Проблема: Сообщения не обрабатываются
**Решение:** Проверьте логи приложений и убедитесь, что консьюмеры подключены к правильным топикам.

## Дополнительная информация

- **Версия Kafka:** 7.4.0 (Confluent Platform)
- **Версия Python:** 3.8+
- **Основные библиотеки:** faust-streaming, kafka-python, aiokafka
- **Формат сериализации:** JSON
- **Хранилище состояний:** Memory store (для разработки)

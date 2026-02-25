import logging
import json
import sys
import random
import time
import os
from kafka import KafkaProducer

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from messages.example_message import MyMessage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

 # Kafka брокеры
BOOTSTRAP_SERVERS = [
    'localhost:9092',
    'localhost:9093',
    'localhost:9094'
]

PRODUCER_CONFIG = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'acks': 'all',  # Гарантия At Least Once
        'retries': 5,    # Количество ретраев
        'retry_backoff_ms': 1000,  # Пауза между ретраями
        'max_in_flight_requests_per_connection': 1,
        #'enable_idempotence': True,  # обеспечит Exactly-Once в сочетании с acks = all, если нужно
        #'compression_type': 'snappy',  # Сжатие, пока не нужно
        'linger_ms': 5,  # Небольшая задержка для батчинга
        'batch_size': 16384,  # Размер батча 16 Кб
}

TOPIC_NAME = 'messages'

class MyProducer:
    def __init__(self):
        self.topic_name = TOPIC_NAME
        self.producer = None
        self._connect()

    def _connect(self):
        try:
            self.producer = KafkaProducer(
                **PRODUCER_CONFIG,
                # вызываем наш кастомный сериализатор, потом заменим на что-то более хитрое, например avro
                # key сразу переводим при отправке сообщения, он скалярный
                value_serializer=lambda v: v.serialize())
            
            logger.info(f'Connected to {BOOTSTRAP_SERVERS} push into topic {self.topic_name}')
        except Exception as e:
            logger.error(f'Failed to connect: {e}')
            raise

    def send_message(self, message: MyMessage) -> bool:
        
        try:
            # При успешной отправке запишем данные созданного сообщения
            def on_send_success(record_metadata):
                logger.info(f'Message sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}')


            # При ошибке отправки зафиксируем ее
            def on_send_error(excp):
                logger.error(f'Error while sending has occured: {excp}')


            # отправка асинхронная, поэтому добавим обработку ответа
            future = self.producer.send(
                topic=self.topic_name,
                key=str(message.sender_id).encode('utf-8'),
                value=message
            )

            # привязываем коллбэки
            future.add_callback(on_send_success).add_errback(on_send_error)

            return True
        except Exception as e:
            logger.error(f'Failed to send: {e}')
            return False
        
    def send_few_messages(self, interval: float = 0.5, msg_count: int = 100, user_num: int = 10):

        logger.info(f'Sending {msg_count} messages for {user_num} customers every {interval} seconds')

        # Генерируем пользователей
        user_ids =  [i for i in range(1, user_num + 1)]

        # Дефолтные сообщения
        contents = [
            'Hello, how are you today?',
            'I''m fine, thanks!',
            '"What''s the weather like?"',
            'Check out this spam message!',
            'Amazing discount 50 off!!!',
            'This is offensive content',
            'Don''t advertise here',
            'I hate this violence',
            'This should be blocked after blocking',
            'This should also be blocked',
            'Spam and advertisement are bad',
            'Promo code: DISCOUNT50',
            'Casino gambling is illegal here'
        ]
        
        message_id = 0

        for i in range(msg_count):
            try:
                # Случайный выбор пользователя
                sender_id = random.choice(user_ids)
                recepient_id = random.choice([i for i in user_ids if i != sender_id])
                
                # инкрементируем для простоты
                message_id += 1

                # Создаем сообщение
                message = MyMessage(message_id, sender_id, recepient_id, content = random.choice(contents))

                self.send_message(message)

                # иммитация задержки отправки, можно использовать для тестирования consumer
                time.sleep(interval)

                # периодически принудительно отправляем сообщения, чтобы не копить и не потерять данные
                if (i + 1) % 10 == 0:
                    self.producer.flush()
                    logger.info(f'Progress: {i + 1}/{msg_count} messages')

            except KeyboardInterrupt:
                logger.info('Production interrupted by user')
                break
            except Exception as e:
                logger.error(f'Error in message production: {e}')

        # Финальный флаш
        self.producer.flush()

    def close(self):
        # Закрытие продюсера
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close(timeout=5)
                logger.info(f'Producer closed')
            except Exception as e:
                logger.error(f'Error closing producer: {e}')

def main():
    # Точка входа для продюсера
    producer = None
    msg_count = int(sys.argv[1]) if len(sys.argv) > 1 else 50

    try:
        # Создаем продюсер
        producer = MyProducer()

        print('Starting message production...')
        print('Press Ctrl+C to stop')
        
        producer.send_few_messages(
            interval=0.3,
            msg_count=msg_count,
            user_num=8
        )

    except KeyboardInterrupt:
        print('\n\nProduction stopped by user')
    except Exception as e:
        logger.error(f'Producer error: {e}')
        print(f'\nError: {e}')
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
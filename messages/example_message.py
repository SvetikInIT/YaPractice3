import json
import faust
from dataclasses import dataclass, asdict
from typing import Optional

@dataclass
class MyMessage(faust.Record, serializer='json'):
    message_id: int
    sender_id: int
    recipient_id: int
    content: str
    
    def serialize(self) -> bytes:
        # Сериализация сообщения в JSON байты
        try:
            # Конвертируем в словарь
            data = asdict(self)
            
            # Сериализуем в JSON
            json_str = json.dumps(data, default=str)
            
            # Выводим на консоль
            print(f'[PRODUCER] Serialized message: {json_str[:100]}...]')
            
            # Сразу переводим в байты
            return json_str.encode('utf-8')
            
        except Exception as e:
            print(f'[ERROR] Serialization failed: {e}')
            raise

    @classmethod
    def deserialize(cls, data: bytes) -> Optional['MyMessage']:
        # Десериализация из JSON байты
        try:
            # Декодируем JSON
            json_str = data.decode('utf-8')
            
            # Кладем в словарь
            data_dict = json.loads(json_str)
            
            # Создаем объект
            message = cls(**data_dict)
            
            # Выводим на консоль
            print(f'[CONSUMER] Deserialized message from {message.sender_id} to {message.recipient_id}')
            
            return message
            
        except json.JSONDecodeError as e:
            print(f'[ERROR] JSON decode failed: {e}')
            return None
        except Exception as e:
            print(f'[ERROR] Deserialization failed: {e}')
            return None
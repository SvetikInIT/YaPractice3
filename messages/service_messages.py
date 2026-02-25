import faust

# Модель для событий блокировки
class BlockEvent(faust.Record, serializer='json'):
    user_id: str
    blocked_user_id: str
    action: str  # 'block' или 'unblock'
    timestamp: float


# Модель для обновления списка запрещенных слов
class CensorWordsUpdate(faust.Record, serializer='json'):
    word: str
    action: str  # 'add' или 'remove'
    timestamp: float
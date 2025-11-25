# CDC Pipeline Examples

Этот репозиторий содержит три примера реализации CDC (Change Data Capture):

## Структура проекта

```
cdc/
├── log_based_cdc/      # Log-Based CDC (Debezium + PostgreSQL WAL)
│   ├── raw_store/      # Raw данные
│   └── DDS/            # Delta таблицы (Data Delivery System)
│       └── users/      # Таблица users
├── trigger_cdc/        # Trigger-Based CDC (PostgreSQL Triggers)
│   ├── raw_store/      # Raw данные
│   └── DDS/            # Delta таблицы
│       └── users/      # Таблица users
└── polling_cdc/        # Polling-Based CDC (External Polling)
    ├── raw_store/      # Raw данные
    └── DDS/            # Delta таблицы
        └── users/      # Таблица users
```

## Примеры

### 1. Log-Based CDC (`log_based_cdc/`)

**Метод:** Чтение из WAL (Write-Ahead Log) через Debezium

**Технологии:**
- PostgreSQL с logical replication
- Debezium
- Kafka
- Spark + Delta Lake

**Преимущества:**
- Минимальная нагрузка на БД
- Высокая производительность
- Подходит для production

**Запуск:**
```batch
cd log_based_cdc
run_all.bat
```

### 2. Trigger-Based CDC (`trigger_cdc/`)

**Метод:** Триггеры PostgreSQL + Python Producer

**Технологии:**
- PostgreSQL с триггерами
- Python скрипт для чтения изменений
- Kafka
- Spark + Delta Lake

**Преимущества:**
- Простота настройки
- Работает с любой версией PostgreSQL
- Полный контроль над процессом

**Запуск:**
```batch
cd trigger_cdc
run_all.bat
```

### 3. Polling-Based CDC (`polling_cdc/`)

**Метод:** Внешний опрос таблицы по полю `updated_at`

**Технологии:**
- PostgreSQL с полем `updated_at` (обновляется приложением)
- Python скрипт для периодического опроса
- Kafka
- Spark + Delta Lake

**Преимущества:**
- Не требует триггеров или WAL
- Работает с любой СУБД
- Простая реализация
- Минимальные требования к БД

**Особенности:**
- Требует, чтобы приложение обновляло `updated_at` при изменениях
- Задержка зависит от интервала опроса
- Может пропустить изменения, если `updated_at` не обновляется

**Запуск:**
```batch
cd polling_cdc
run_all.bat
```

## Структура данных

Каждый пример имеет свою собственную структуру данных:
- `log_based_cdc/raw_store/` - raw данные для log-based примера
- `log_based_cdc/DDS/` - Delta таблицы (Data Delivery System) для log-based примера
- `trigger_cdc/raw_store/` - raw данные для trigger-based примера  
- `trigger_cdc/DDS/` - Delta таблицы для trigger-based примера
- `polling_cdc/raw_store/` - raw данные для polling-based примера
- `polling_cdc/DDS/` - Delta таблицы для polling-based примера

Каждый пример работает независимо и не конфликтует с другим. Все используют разные порты Docker.

## Сравнение методов

| Аспект | Log-Based | Trigger-Based | Polling-Based |
|--------|-----------|---------------|---------------|
| Нагрузка на БД | Минимальная | Средняя | Низкая |
| Производительность | Высокая | Средняя | Зависит от интервала |
| Масштабируемость | Отличная | Ограниченная | Хорошая |
| Сложность настройки | Средняя | Простая | Очень простая |
| Задержка | Минимальная | Низкая | Зависит от интервала |
| Требования к БД | WAL + logical replication | Любая версия | Любая СУБД |
| Production ready | Да | Для небольших систем | Для небольших систем |

## Документация

- `log_based_cdc/MANUAL_STEPS.md` - Шпаргалка для ручного запуска Log-Based CDC
- `trigger_cdc/README.md` - Подробная документация Trigger-Based CDC


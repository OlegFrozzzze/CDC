# Как работает Log-Based CDC

## Общая концепция

Log-Based CDC использует **Write-Ahead Log (WAL)** PostgreSQL для отслеживания изменений в реальном времени. Debezium читает транзакционный лог БД и автоматически отправляет все изменения в Kafka. Это наиболее производительный и надежный подход для production.

---

## Шаг 1: Инициализация БД

```sql
-- Создается таблица users
INSERT INTO users (ID, name, email, age) VALUES 
    (1001, 'John Doe', 'john@example.com', 30),
    (1002, 'Jane Smith', 'jane@example.com', 25);
```

**Результат в БД:**
```
ID    | name      | email              | age | created_at
------|-----------|--------------------|-----|-------------------
1001  | John Doe  | john@example.com  | 30  | 2025-11-24 20:56:30
1002  | Jane Smith| jane@example.com   | 25  | 2025-11-24 20:56:30
```

**Важно:** PostgreSQL должен быть настроен с `wal_level=logical` для logical replication.

---

## Шаг 2: Создание Publication и Replication Slot

```sql
CREATE PUBLICATION debezium_publication FOR TABLE users;
```

**Что это делает:**
- Создает **publication** - список таблиц, изменения которых нужно отслеживать
- PostgreSQL начинает записывать изменения этих таблиц в WAL в специальном формате
- Debezium будет читать эти изменения через replication slot

**Replication Slot:**
- Создается автоматически Debezium при первом подключении
- Гарантирует, что изменения не будут удалены из WAL, пока Debezium их не прочитает
- Имя: `debezium_slot` (из конфигурации)

---

## Шаг 3: Запуск Debezium Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium-connector-config.json
```

**Что происходит:**

1. **Debezium подключается к PostgreSQL:**
   - Читает конфигурацию из `debezium-connector-config.json`
   - Подключается к БД через logical replication
   - Создает replication slot (если не существует)

2. **Начальный снимок (Initial Snapshot):**
   - Debezium читает все существующие записи из таблицы `users`
   - Отправляет их в Kafka с операцией `__op: "r"` (read/snapshot)

3. **Непрерывное чтение WAL:**
   - Debezium начинает читать транзакционный лог PostgreSQL
   - Каждое изменение (INSERT/UPDATE/DELETE) автоматически отправляется в Kafka

**Результат в Kafka:**
```
Topic: postgres-server.public.users

Message 1: {"id": 1001, "name": "John Doe", ..., "__op": "r"}  ← snapshot
Message 2: {"id": 1002, "name": "Jane Smith", ..., "__op": "r"}  ← snapshot
```

---

## Шаг 4: Spark обрабатывает данные

1. **Spark читает из Kafka** → сохраняет в `raw_store/version_1`
2. **Spark обрабатывает `raw_store`** → создает Delta таблицу в `DDS/users`

**Результат в Delta таблице:**
```
ID    | name      | email              | age | created_at
------|-----------|--------------------|-----|-------------------
1001  | John Doe  | john@example.com  | 30  | 2025-11-24 20:56:30
1002  | Jane Smith| jane@example.com   | 25  | 2025-11-24 20:56:30
```

---

## Шаг 5: Добавление новых данных в БД

```sql
INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);
UPDATE users SET email = 'john.updated@example.com', age = 31 WHERE ID = 1001;
DELETE FROM users WHERE ID = 1002;
```

**Что происходит в реальном времени:**

1. **INSERT:**
   - PostgreSQL записывает INSERT в WAL
   - Debezium читает из WAL → отправляет в Kafka с `__op: "c"` (create)

2. **UPDATE:**
   - PostgreSQL записывает UPDATE в WAL
   - Debezium читает из WAL → отправляет в Kafka с `__op: "u"` (update)

3. **DELETE:**
   - PostgreSQL записывает DELETE в WAL
   - Debezium читает из WAL → отправляет в Kafka с `__op: "d"` (delete)
   - После DELETE сообщения отправляется **tombstone** (null value) для очистки

**Результат в Kafka (автоматически, без запуска producer):**
```
Message 3: {"id": 1003, "name": "New User", ..., "__op": "c"}  ← INSERT
Message 4: {"id": 1001, "name": "John Doe", "email": "john.updated@...", ..., "__op": "u"}  ← UPDATE
Message 5: {"id": 1002, ..., "__op": "d"}  ← DELETE
Message 6: null (tombstone для 1002)
```

---

## Шаг 6: Spark обрабатывает новые данные

1. **Spark читает новые сообщения из Kafka** → добавляет в `raw_store`
2. **Spark обрабатывает `raw_store`** → обновляет Delta таблицу (merge)

**Результат в Delta таблице:**
```
ID    | name      | email                  | age | created_at
------|-----------|------------------------|-----|-------------------
1001  | John Doe  | john.updated@example...| 31  | 2025-11-24 20:56:30
1003  | New User  | newuser@example.com    | 28  | 2025-11-24 21:10:00
(1002 удалена из Delta таблицы через merge с операцией DELETE)
```

---

## Ключевые моменты

### 1. WAL (Write-Ahead Log)
- PostgreSQL записывает все изменения в транзакционный лог
- Debezium читает этот лог в реальном времени
- Изменения не теряются, даже если Kafka временно недоступна

### 2. Replication Slot
- Гарантирует, что WAL не будет очищен, пока Debezium не прочитает изменения
- Позволяет восстановиться после сбоев
- Хранится в PostgreSQL: `SELECT * FROM pg_replication_slots;`

### 3. Debezium работает непрерывно
- Не нужно запускать вручную
- Автоматически отслеживает все изменения
- Отправляет в Kafka в реальном времени

### 4. Начальный снимок (Snapshot)
- При первом запуске Debezium делает снимок всех данных
- Отправляет существующие записи с `__op: "r"` (read)
- После снимка переходит на чтение WAL

### 5. Tombstone сообщения
- После DELETE Debezium отправляет сообщение с null value
- Используется для очистки данных в системах downstream
- Spark может использовать для удаления записей из Delta таблицы

---

## Схема работы

```
┌─────────────────┐
│  БД (PostgreSQL)│
│  users table    │
│                 │
│  WAL (логи)     │
│  INSERT/UPDATE/ │
│  DELETE         │
└────────┬────────┘
         │
         │ Logical Replication
         │ (читает WAL)
         │
         ▼
┌─────────────────┐
│  Debezium       │
│  (непрерывно)   │
│  1. Читает WAL  │
│  2. → Kafka     │
│  3. Автоматически│
└────────┬────────┘
         │
         │ JSON сообщения
         │
         ▼
┌─────────────────┐
│  Kafka          │
│  postgres-server│
│  .public.users  │
└────────┬────────┘
         │
         │ Spark читает
         │
         ▼
┌─────────────────┐
│  Spark          │
│  1. raw_store   │
│  2. DDS/users   │
└─────────────────┘
```

---

## Преимущества

- Минимальная нагрузка на БД (чтение WAL, не запросы к таблицам)
- Высокая производительность
- Отслеживает все операции (INSERT/UPDATE/DELETE)
- Работает в реальном времени
- Надежность (replication slot гарантирует доставку)
- Production-ready

## Недостатки

- Требует PostgreSQL с `wal_level=logical`
- Более сложная настройка (Debezium, replication slot)
- Replication slot занимает место на диске (если Kafka недоступна долго)

---

## Конфигурация Debezium

```json
{
  "name": "postgres-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "postgres",
    "database.password": "postgres",
    "database.dbname": "testdb",
    "database.server.name": "postgres-server",
    "table.include.list": "public.users",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "debezium_publication"
  }
}
```

**Важные параметры:**
- `slot.name` - имя replication slot
- `publication.name` - имя publication (должно совпадать с созданным в БД)
- `plugin.name` - плагин для logical replication (`pgoutput` для PostgreSQL 10+)


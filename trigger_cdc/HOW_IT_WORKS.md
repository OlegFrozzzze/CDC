# Как работает Trigger-Based CDC

## Общая концепция

Trigger-Based CDC использует **триггеры PostgreSQL** для автоматической записи всех изменений в отдельную таблицу `cdc_changes_log`. Python producer периодически читает эту таблицу, отправляет изменения в Kafka и помечает их как обработанные. Это простой подход, который работает с любой версией PostgreSQL.

---

## Шаг 1: Инициализация БД

```sql
-- Создается таблица для логов изменений
CREATE TABLE cdc_changes_log (
    change_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation CHAR(1) NOT NULL,  -- 'I' = INSERT, 'U' = UPDATE, 'D' = DELETE
    record_id BIGINT NOT NULL,
    old_data JSONB,
    new_data JSONB,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Создается таблица users
CREATE TABLE users (
    ID BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаются триггеры на INSERT, UPDATE, DELETE
CREATE TRIGGER trigger_users_insert AFTER INSERT ON users ...
CREATE TRIGGER trigger_users_update AFTER UPDATE ON users ...
CREATE TRIGGER trigger_users_delete AFTER DELETE ON users ...
```

**Важно:** Триггеры автоматически записывают все изменения в `cdc_changes_log`.

---

## Шаг 2: Добавление начальных данных

```sql
INSERT INTO users (ID, name, email, age) VALUES 
    (1001, 'John Doe', 'john@example.com', 30),
    (1002, 'Jane Smith', 'jane@example.com', 25);
```

**Что происходит автоматически:**

1. **Триггер срабатывает на INSERT:**
   - Записывает в `cdc_changes_log`:
     ```
     change_id | table_name | operation | record_id | new_data                    | processed
     ----------|------------|-----------|-----------|-----------------------------|----------
     1         | users      | I         | 1001      | {"id": 1001, "name": ...}   | false
     2         | users      | I         | 1002      | {"id": 1002, "name": ...}   | false
     ```

2. **Результат в БД:**
   ```
   users:
   ID    | name      | email              | age | created_at
   ------|-----------|--------------------|-----|-------------------
   1001  | John Doe  | john@example.com  | 30  | 2025-11-24 20:56:30
   1002  | Jane Smith| jane@example.com   | 25  | 2025-11-24 20:56:30
   ```

---

## Шаг 3: Запуск CDC Producer

```bash
# Producer запускается автоматически в контейнере и работает непрерывно
```

**Что происходит внутри producer (непрерывный цикл):**

1. **Читает необработанные изменения:**
   ```sql
   SELECT * FROM cdc_changes_log 
   WHERE processed = FALSE 
   ORDER BY changed_at, change_id 
   FOR UPDATE SKIP LOCKED
   ```
   - Находит записи с `processed = FALSE`
   - Использует `FOR UPDATE SKIP LOCKED` для безопасной параллельной обработки

2. **Отправляет изменения в Kafka:**
   - Запись 1 (INSERT 1001) → Kafka с `__op: "c"` (create)
   - Запись 2 (INSERT 1002) → Kafka с `__op: "c"` (create)

3. **Помечает как обработанные:**
   ```sql
   UPDATE cdc_changes_log 
   SET processed = TRUE 
   WHERE change_id = 1;
   ```
   - После успешной отправки в Kafka помечает `processed = TRUE`

4. **Ждет интервал** (по умолчанию 2 секунды) и повторяет цикл

**Результат в Kafka:**
```
Message 1: {"id": 1001, "name": "John Doe", ..., "__op": "c"}
Message 2: {"id": 1002, "name": "Jane Smith", ..., "__op": "c"}
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

**Что происходит автоматически:**

1. **INSERT:**
   - Триггер записывает в `cdc_changes_log`:
     ```
     change_id | operation | record_id | new_data                    | processed
     ----------|-----------|-----------|-----------------------------|----------
     3         | I         | 1003      | {"id": 1003, "name": ...}   | false
     ```

2. **UPDATE:**
   - Триггер записывает в `cdc_changes_log`:
     ```
     change_id | operation | record_id | old_data                    | new_data                    | processed
     ----------|-----------|-----------|-----------------------------|-----------------------------|----------
     4         | U         | 1001      | {"id": 1001, "email": ...}  | {"id": 1001, "email": ...} | false
     ```

3. **DELETE:**
   - Триггер записывает в `cdc_changes_log`:
     ```
     change_id | operation | record_id | old_data                    | processed
     ----------|-----------|-----------|-----------------------------|----------
     5         | D         | 1002      | {"id": 1002, "name": ...}   | false
     ```

---

## Шаг 6: Producer обрабатывает новые изменения

**Producer (работает непрерывно, каждые 2 секунды):**

1. **Читает новые записи из `cdc_changes_log`:**
   - Находит записи 3, 4, 5 с `processed = FALSE`

2. **Отправляет в Kafka:**
   - Запись 3 (INSERT 1003) → Kafka с `__op: "c"`
   - Запись 4 (UPDATE 1001) → Kafka с `__op: "u"`
   - Запись 5 (DELETE 1002) → Kafka с `__op: "d"`

3. **Помечает как обработанные:**
   - `UPDATE cdc_changes_log SET processed = TRUE WHERE change_id IN (3, 4, 5)`

**Результат в Kafka:**
```
Message 3: {"id": 1003, "name": "New User", ..., "__op": "c"}  ← INSERT
Message 4: {"id": 1001, "name": "John Doe", "email": "john.updated@...", ..., "__op": "u"}  ← UPDATE
Message 5: {"id": 1002, ..., "__op": "d"}  ← DELETE
```

---

## Шаг 7: Spark обрабатывает новые данные

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

### 1. Триггеры PostgreSQL
- Автоматически срабатывают на INSERT/UPDATE/DELETE
- Записывают изменения в `cdc_changes_log` с полными данными (old_data, new_data)
- Не требуют изменений в приложении

### 2. Таблица `cdc_changes_log`
- Хранит все изменения с метаданными
- Поле `processed` отслеживает, какие изменения уже отправлены в Kafka
- Индекс на `(processed, changed_at)` для быстрого поиска

### 3. Producer работает непрерывно
- Цикл: читает → отправляет → помечает → ждет → повторяет
- Интервал опроса: 2 секунды (настраивается через `POLL_INTERVAL`)
- Использует `FOR UPDATE SKIP LOCKED` для безопасной параллельной обработки

### 4. Транзакционность
- Producer использует транзакции: сначала отправляет в Kafka, потом помечает `processed = TRUE`
- Если Kafka недоступна, изменения остаются необработанными и будут отправлены позже

### 5. Все операции отслеживаются
- INSERT → `operation = 'I'` → Kafka `__op: "c"`
- UPDATE → `operation = 'U'` → Kafka `__op: "u"`
- DELETE → `operation = 'D'` → Kafka `__op: "d"`

---

## Схема работы

```
┌─────────────────┐
│  БД (PostgreSQL)│
│  users table    │
│                 │
│  Триггеры:      │
│  INSERT/UPDATE/ │
│  DELETE         │
└────────┬────────┘
         │
         │ Автоматически записывает
         │
         ▼
┌─────────────────┐
│  cdc_changes_log│
│  table          │
│  processed=false│
└────────┬────────┘
         │
         │ SQL: WHERE processed = FALSE
         │
         ▼
┌─────────────────┐
│  Producer       │
│  (непрерывно)   │
│  1. Читает лог  │
│  2. → Kafka     │
│  3. processed=  │
│     TRUE        │
│  4. Ждет 2 сек  │
│  5. Повторяет   │
└────────┬────────┘
         │
         │ JSON сообщения
         │
         ▼
┌─────────────────┐
│  Kafka          │
│  trigger-cdc.   │
│  users          │
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

- Простота настройки (только триггеры)
- Работает с любой версией PostgreSQL
- Не требует WAL или logical replication
- Отслеживает все операции (INSERT/UPDATE/DELETE)
- Полный контроль над процессом
- Хранит историю изменений в `cdc_changes_log`

## Недостатки

- Средняя нагрузка на БД (триггеры выполняются при каждом изменении)
- Таблица `cdc_changes_log` растет (нужна очистка старых записей)
- Задержка зависит от интервала опроса producer (по умолчанию 2 секунды)
- Для больших нагрузок может быть узким местом

---

## Функция триггера

```sql
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO cdc_changes_log (table_name, operation, record_id, new_data)
        VALUES ('users', 'I', NEW.ID, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO cdc_changes_log (table_name, operation, record_id, old_data, new_data)
        VALUES ('users', 'U', NEW.ID, row_to_json(OLD)::jsonb, row_to_json(NEW)::jsonb);
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO cdc_changes_log (table_name, operation, record_id, old_data)
        VALUES ('users', 'D', OLD.ID, row_to_json(OLD)::jsonb);
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

**Что делает:**
- `INSERT` → сохраняет `new_data` (новую запись)
- `UPDATE` → сохраняет `old_data` (старые данные) и `new_data` (новые данные)
- `DELETE` → сохраняет `old_data` (удаленную запись)


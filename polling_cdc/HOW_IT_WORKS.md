# Как работает Polling-Based CDC

## Общая концепция

Polling-Based CDC работает как **одноразовая задача** (job), которая запускается по требованию или по расписанию (например, через Airflow). Producer проверяет таблицу один раз, находит изменения по полю `updated_at`, отправляет их в Kafka и завершает работу.

---

## Шаг 1: Инициализация БД

```sql
-- В БД создается таблица users с полем updated_at
INSERT INTO users (ID, name, email, age) VALUES 
    (1001, 'John Doe', 'john@example.com', 30),
    (1002, 'Jane Smith', 'jane@example.com', 25);
```

**Результат в БД:**
```
ID    | name      | email              | age | created_at          | updated_at
------|-----------|--------------------|-----|---------------------|-------------------
1001  | John Doe  | john@example.com  | 30  | 2025-11-24 20:56:30 | 2025-11-24 20:56:30
1002  | Jane Smith| jane@example.com   | 25  | 2025-11-24 20:56:30 | 2025-11-24 20:56:30
```

**Важно:** Поле `updated_at` должно обновляться приложением при каждом изменении записи.

---

## Шаг 2: Первый запуск Producer

```bash
docker exec polling-cdc-producer python polling_producer.py
```

### Что происходит внутри:

1. **Читает `last_polled_timestamp` из файла** `/tmp/last_polled_timestamp.txt`
   - Если файла нет → возвращает `текущее_время - 1 час` (например: `2025-11-24 19:56:30`)

2. **Выполняет SQL запрос:**
   ```sql
   SELECT * FROM users 
   WHERE updated_at > '2025-11-24 19:56:30'
   ORDER BY updated_at, ID
   ```
   - Находит обе записи (1001 и 1002), так как их `updated_at = 20:56:30` > `19:56:30`

3. **Отправляет найденные записи в Kafka:**
   - Запись 1001 → Kafka (операция 'c' - create)
   - Запись 1002 → Kafka (операция 'c' - create)

4. **Сохраняет `last_polled_timestamp`:**
   - Берет максимальный `updated_at` из обработанных записей: `2025-11-24 20:56:30`
   - Сохраняет в файл: `/tmp/last_polled_timestamp.txt`

5. **Завершает работу**

---

## Шаг 3: Spark обрабатывает данные

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

## Шаг 4: Добавление новых данных в БД

```sql
INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);
UPDATE users SET email = 'john.updated@example.com', age = 31, updated_at = CURRENT_TIMESTAMP WHERE ID = 1001;
DELETE FROM users WHERE ID = 1002;
```

**Результат в БД:**
```
ID    | name      | email                  | age | created_at          | updated_at
------|-----------|------------------------|-----|---------------------|-------------------
1001  | John Doe  | john.updated@example...| 31  | 2025-11-24 20:56:30 | 2025-11-24 21:10:00  ← ОБНОВЛЕНО
1003  | New User  | newuser@example.com    | 28  | 2025-11-24 21:10:00 | 2025-11-24 21:10:00  ← НОВАЯ
(1002 удалена)
```

**Важно:** При UPDATE нужно явно обновлять `updated_at = CURRENT_TIMESTAMP` (в production это делает приложение).

---

## Шаг 5: Повторный запуск Producer

```bash
docker exec polling-cdc-producer python polling_producer.py
```

### Что происходит:

1. **Читает `last_polled_timestamp` из файла:**
   - `2025-11-24 20:56:30` (сохранено после первого запуска)

2. **Выполняет SQL запрос:**
   ```sql
   SELECT * FROM users 
   WHERE updated_at > '2025-11-24 20:56:30'
   ```
   - Находит только записи с `updated_at > 20:56:30`:
     - 1001 (updated_at = 21:10:00) ← обновлена
     - 1003 (updated_at = 21:10:00) ← новая
   - 1002 не найдена (удалена, но polling не видит удаления)

3. **Отправляет найденные записи в Kafka:**
   - Запись 1001 (UPDATE) → Kafka (операция 'u' - update)
   - Запись 1003 (INSERT) → Kafka (операция 'c' - create)

4. **Сохраняет новый `last_polled_timestamp`:**
   - Максимальный `updated_at`: `2025-11-24 21:10:00`
   - Сохраняет в файл

5. **Завершает работу**

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

### 1. `last_polled_timestamp`
- Хранится в файле `/tmp/last_polled_timestamp.txt` внутри контейнера
- Сохраняется после каждого успешного запуска
- Используется для следующего запуска

### 2. SQL запрос
- Всегда ищет: `WHERE updated_at > last_polled_timestamp`
- Находит только новые/измененные записи
- Не обрабатывает одни и те же записи дважды

### 3. Producer - одноразовая задача
- Запускается вручную (или по расписанию, как в Airflow)
- Делает одну проверку БД
- Завершается

### 4. Ограничение: DELETE не отслеживается
- Удаленные записи исчезают из таблицы
- Polling их не видит
- Для отслеживания удалений нужен другой подход (например, soft delete с `is_deleted` флагом)

---

## Схема работы

```
┌─────────────────┐
│  БД (PostgreSQL)│
│  users table    │
│  updated_at     │
└────────┬────────┘
         │
         │ SQL: WHERE updated_at > last_polled
         │
         ▼
┌─────────────────┐
│  Producer       │
│  (одноразовая)  │
│  1. Проверка БД │
│  2. → Kafka     │
│  3. Сохранить   │
│     timestamp   │
│  4. Завершение  │
└────────┬────────┘
         │
         │ JSON сообщения
         │
         ▼
┌─────────────────┐
│  Kafka          │
│  polling-cdc.   │
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

- Простота реализации
- Не требует триггеров или WAL
- Работает с любой СУБД
- Минимальные требования к БД

## Недостатки

- Задержка зависит от частоты запуска
- Требует, чтобы приложение обновляло `updated_at`
- Не отслеживает DELETE операции напрямую
- Может пропустить изменения, если `updated_at` не обновляется


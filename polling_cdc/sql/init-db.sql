-- Создание тестовой таблицы users с полем updated_at для отслеживания изменений
-- В polling-based CDC мы полагаемся на поле updated_at, которое обновляется приложением
-- Триггеры НЕ являются частью polling-based подхода - это внешний опрос таблицы
CREATE TABLE IF NOT EXISTS users (
    ID BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание индекса для быстрого поиска изменений по updated_at
-- Это критично для производительности polling-запросов
CREATE INDEX IF NOT EXISTS idx_users_updated_at ON users(updated_at);

-- ПРИМЕЧАНИЕ: В реальном приложении поле updated_at должно обновляться самим приложением:
-- UPDATE users SET email = 'new@example.com', updated_at = CURRENT_TIMESTAMP WHERE ID = 1001;
--
-- Для демонстрации мы будем вручную обновлять updated_at в SQL командах.
-- В production это должно делаться на уровне приложения.

-- Вставка тестовых данных
INSERT INTO users (ID, name, email, age) VALUES 
    (1001, 'John Doe', 'john@example.com', 30),
    (1002, 'Jane Smith', 'jane@example.com', 25);


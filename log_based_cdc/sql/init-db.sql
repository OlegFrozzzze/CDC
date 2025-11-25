-- Создание тестовой таблицы для проверки CDC
CREATE TABLE IF NOT EXISTS users (
    ID BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Вставка тестовых данных
INSERT INTO users (ID, name, email, age) VALUES 
    (1001, 'John Doe', 'john@example.com', 30),
    (1002, 'Jane Smith', 'jane@example.com', 25);



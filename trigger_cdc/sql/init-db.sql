-- Создание таблицы для хранения логов изменений (CDC log table)
CREATE TABLE IF NOT EXISTS cdc_changes_log (
    change_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation CHAR(1) NOT NULL, -- 'I' = INSERT, 'U' = UPDATE, 'D' = DELETE
    record_id BIGINT NOT NULL,
    old_data JSONB,
    new_data JSONB,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);

-- Создание индекса для быстрого поиска необработанных изменений
CREATE INDEX IF NOT EXISTS idx_cdc_changes_log_processed ON cdc_changes_log(processed, changed_at);

-- Создание тестовой таблицы users
CREATE TABLE IF NOT EXISTS users (
    ID BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Функция для логирования изменений
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

-- Создание триггера на INSERT
DROP TRIGGER IF EXISTS trigger_users_insert ON users;
CREATE TRIGGER trigger_users_insert
    AFTER INSERT ON users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_changes();

-- Создание триггера на UPDATE
DROP TRIGGER IF EXISTS trigger_users_update ON users;
CREATE TRIGGER trigger_users_update
    AFTER UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_changes();

-- Создание триггера на DELETE
DROP TRIGGER IF EXISTS trigger_users_delete ON users;
CREATE TRIGGER trigger_users_delete
    AFTER DELETE ON users
    FOR EACH ROW
    EXECUTE FUNCTION log_user_changes();

-- Вставка тестовых данных
INSERT INTO users (ID, name, email, age) VALUES 
    (1001, 'John Doe', 'john@example.com', 30),
    (1002, 'Jane Smith', 'jane@example.com', 25);


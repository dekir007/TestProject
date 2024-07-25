DROP ROLE IF EXISTS testproject;

CREATE ROLE testproject WITH LOGIN PASSWORD '123123';

GRANT CONNECT ON DATABASE postgres TO testproject;
GRANT USAGE, CREATE ON SCHEMA public TO testproject;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO testproject;
-- нужен для автоматической выдачи прав на новые таблицы
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO testproject;
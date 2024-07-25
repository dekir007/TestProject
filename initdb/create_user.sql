DO
$$
BEGIN
    IF EXISTS (
        SELECT FROM pg_catalog.pg_roles
        WHERE rolname = 'new_user') THEN

        DROP ROLE new_user;
    END IF;
END
$$;

CREATE ROLE new_user WITH LOGIN PASSWORD 'new_password';

GRANT CONNECT ON DATABASE ${DBNAME} TO new_user;
GRANT USAGE ON SCHEMA public TO new_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO new_user;
--ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO new_user;

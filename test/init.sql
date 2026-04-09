-- PostgreSQL initialization script for pg_mqtt_pub Docker environment
-- Runs via init container after PostgreSQL is ready

-- Setup testdb (default database) - create extensions here
\c testdb
CREATE EXTENSION IF NOT EXISTS pg_cron;
DROP SCHEMA IF EXISTS mqtt_pub CASCADE;
CREATE EXTENSION pg_mqtt_pub;

-- Stress test helper function
CREATE OR REPLACE FUNCTION stress_test_mqtt(
    message_count INT,
    batch_size INT DEFAULT 1,
    delay_ms INT DEFAULT 1,
    qos INT DEFAULT 0
) RETURNS TABLE(msg_num INT, result BOOLEAN) AS $$
DECLARE
    i INT;
    res BOOLEAN;
BEGIN
    FOR i IN 1..message_count LOOP
        res := mqtt_pub.mqtt_publish('stress/test', 'msg-' || i::text, qos);
        RETURN QUERY SELECT i, res;

        -- Delay after each batch (but not after the last message)
        IF i % batch_size = 0 AND i < message_count THEN
            PERFORM pg_sleep(delay_ms::FLOAT / 1000.0);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

\echo 'Extensions installed successfully in testdb!'
\echo 'Stress test function available: SELECT * FROM stress_test_mqtt(100);'

CREATE TABLE IF NOT EXISTS telemetry(
    id SERIAL PRIMARY KEY,
    user_id SERIAL,
    prosthesis_type TEXT,
    muscle_group TEXT,
    signal_frequency INTEGER,
    signal_duration INTEGER,
    signal_amplitude DECIMAL(5,2),
    signal_time TIMESTAMP WITHOUT TIME ZONE
);

COPY telemetry(user_id, prosthesis_type, muscle_group, signal_frequency, signal_duration, signal_amplitude, signal_time)
FROM '/docker-entrypoint-initdb.d/telemetry.csv'
DELIMITER ','
CSV HEADER;
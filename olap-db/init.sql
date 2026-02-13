CREATE TABLE IF NOT EXISTS daily_prosthesis_reports (
    report_date Date,
    user_id UInt32,
    prosthesis_type String,
    muscle_group String,
    total_signals UInt32,
    avg_frequency Float32,
    avg_duration Float32,
    avg_amplitude Float32,
    max_amplitude Float32,
    min_amplitude Float32,
    peak_activity_hour UInt8,
    user_name String,
    user_email String,
    user_country String,
    user_age UInt8,
    user_gender String
) ENGINE = MergeTree()
ORDER BY (report_date, user_id, prosthesis_type)
PARTITION BY toYYYYMM(report_date);

CREATE TABLE IF NOT EXISTS monthly_prosthesis_reports (
    report_month Date,
    user_id UInt32,
    prosthesis_type String,
    muscle_group String,
    total_signals UInt32,
    avg_daily_signals Float32,
    avg_frequency Float32,
    avg_duration Float32,
    avg_amplitude Float32,
    max_amplitude_daily Float32,
    min_amplitude_daily Float32,
    most_active_day UInt8,
    user_name String,
    user_email String,
    user_country String,
    user_age UInt8,
    user_gender String,
    days_used UInt8
) ENGINE = MergeTree()
ORDER BY (report_month, user_id, prosthesis_type)
PARTITION BY toYYYYMM(report_month);
"""
ETL DAG для генерации отчетов по работе протезов BionicPRO
Объединяет данные из CRM и телеметрии
"""

from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client as ClickHouseClient

# Настройка логирования
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'bionicpro_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Получение настроек из переменных окружения
def get_env_variables():
    """Получение всех необходимых переменных окружения"""
    env_vars = {
        'CRM_DB_HOST': os.getenv('CRM_DB_HOST', 'postgres-crm'),
        'CRM_DB_PORT': int(os.getenv('CRM_DB_PORT', 5432)),
        'CRM_DB_NAME': os.getenv('CRM_DB_NAME', 'crm_db'),
        'CRM_DB_USER': os.getenv('CRM_DB_USER', 'bionicpro'),
        'CRM_DB_PASSWORD': os.getenv('CRM_DB_PASSWORD', 'secure_password'),
        
        'TELEMETRY_DB_HOST': os.getenv('TELEMETRY_DB_HOST', 'postgres-telemetry'),
        'TELEMETRY_DB_PORT': int(os.getenv('TELEMETRY_DB_PORT', 5432)),
        'TELEMETRY_DB_NAME': os.getenv('TELEMETRY_DB_NAME', 'telemetry_db'),
        'TELEMETRY_DB_USER': os.getenv('TELEMETRY_DB_USER', 'bionicpro'),
        'TELEMETRY_DB_PASSWORD': os.getenv('TELEMETRY_DB_PASSWORD', 'secure_password'),
        
        'CLICKHOUSE_HOST': os.getenv('CLICKHOUSE_HOST', 'clickhouse'),
        'CLICKHOUSE_PORT': int(os.getenv('CLICKHOUSE_PORT', 9000)),
        'CLICKHOUSE_DB': os.getenv('CLICKHOUSE_DB', 'bionicpro_reports'),
        'CLICKHOUSE_USER': os.getenv('CLICKHOUSE_USER', 'default'),
        'CLICKHOUSE_PASSWORD': os.getenv('CLICKHOUSE_PASSWORD', ''),
    }
    
    # Логируем полученные настройки (без паролей)
    logger.info(f"CRM DB: {env_vars['CRM_DB_HOST']}:{env_vars['CRM_DB_PORT']}/{env_vars['CRM_DB_NAME']}")
    logger.info(f"Telemetry DB: {env_vars['TELEMETRY_DB_HOST']}:{env_vars['TELEMETRY_DB_PORT']}/{env_vars['TELEMETRY_DB_NAME']}")
    logger.info(f"ClickHouse: {env_vars['CLICKHOUSE_HOST']}:{env_vars['CLICKHOUSE_PORT']}/{env_vars['CLICKHOUSE_DB']}")
    
    return env_vars

def create_custom_postgres_hook(connection_config, hook_name="custom_postgres"):
    """Создание кастомного PostgresHook с динамическими настройками"""
    from airflow.models import Connection
    from airflow.utils.db import create_session
    
    env_vars = get_env_variables()
    
    # Определяем параметры подключения в зависимости от типа
    if connection_config == 'crm':
        conn_id = f"{hook_name}_crm"
        host = env_vars['CRM_DB_HOST']
        port = env_vars['CRM_DB_PORT']
        schema = env_vars['CRM_DB_NAME']
        login = env_vars['CRM_DB_USER']
        password = env_vars['CRM_DB_PASSWORD']
    elif connection_config == 'telemetry':
        conn_id = f"{hook_name}_telemetry"
        host = env_vars['TELEMETRY_DB_HOST']
        port = env_vars['TELEMETRY_DB_PORT']
        schema = env_vars['TELEMETRY_DB_NAME']
        login = env_vars['TELEMETRY_DB_USER']
        password = env_vars['TELEMETRY_DB_PASSWORD']
    else:
        raise ValueError(f"Unknown connection config: {connection_config}")
    
    # Создаем или обновляем connection в Airflow метаданных
    with create_session() as session:
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        
        if not conn:
            conn = Connection(
                conn_id=conn_id,
                conn_type='postgres',
                host=host,
                port=port,
                schema=schema,
                login=login,
                password=password
            )
            session.add(conn)
            session.commit()
            logger.info(f"Created new connection: {conn_id}")
        else:
            # Обновляем существующее соединение
            conn.host = host
            conn.port = port
            conn.schema = schema
            conn.login = login
            conn.password = password
            session.commit()
            logger.info(f"Updated existing connection: {conn_id}")
    
    # Возвращаем hook с созданным соединением
    return PostgresHook(postgres_conn_id=conn_id)

def extract_crm_data(**context):
    """Извлечение данных о пользователях из CRM"""
    logger.info("Начало извлечения данных из CRM")
    
    # Создаем hook с динамическими настройками
    crm_hook = create_custom_postgres_hook('crm', 'bionicpro')
    
    query = """
    SELECT 
        id,
        name,
        email,
        age,
        gender,
        country,
        address,
        phone
    FROM users
    WHERE id IS NOT NULL
    """
    
    try:
        crm_data = crm_hook.get_records(query)
        logger.info(f"Извлечено {len(crm_data)} записей из CRM")
        
        # Преобразование в словарь для удобства
        crm_dict = {str(row[0]): {
            'name': row[1],
            'email': row[2],
            'age': row[3],
            'gender': row[4],
            'country': row[5],
            'address': row[6],
            'phone': row[7]
        } for row in crm_data}
        
        context['ti'].xcom_push(key='crm_data', value=crm_dict)
        return crm_dict
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных из CRM: {str(e)}")
        raise

def extract_telemetry_data(**context):
    """Извлечение данных телеметрии за последние сутки"""
    logger.info("Начало извлечения данных телеметрии")
    
    # Создаем hook с динамическими настройками
    telemetry_hook = create_custom_postgres_hook('telemetry', 'bionicpro')
    
    # Извлекаем данные за последние 24 часа для ежедневного отчета
    # WHERE signal_time >= NOW() - INTERVAL '24 HOURS'
    # Для тестов возьмем конкретную дату
    query = """
    SELECT 
        user_id,
        prosthesis_type,
        muscle_group,
        signal_frequency,
        signal_duration,
        signal_amplitude,
        signal_time
    FROM telemetry
    WHERE signal_time::date = '2025-03-23'::date
    AND user_id IS NOT NULL
    ORDER BY signal_time
    """
    
    try:
        telemetry_data = telemetry_hook.get_records(query)
        logger.info(f"Извлечено {len(telemetry_data)} записей телеметрии")
        
        context['ti'].xcom_push(key='telemetry_data', value=telemetry_data)
        return telemetry_data
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении данных телеметрии: {str(e)}")
        raise

def transform_daily_data(**context):
    """Трансформация данных для ежедневного отчета"""
    logger.info("Начало трансформации данных для ежедневного отчета")
    
    # Получаем данные из предыдущих задач
    crm_data = context['ti'].xcom_pull(task_ids='extract_crm_data', key='crm_data')
    telemetry_data = context['ti'].xcom_pull(task_ids='extract_telemetry_data', key='telemetry_data')
    
    if not telemetry_data:
        logger.warning("Нет данных телеметрии для обработки")
        context['ti'].xcom_push(key='daily_reports', value=[])
        return []
    
    # Группируем данные по пользователю, типу протеза и дате
    grouped_data = {}
    
    for record in telemetry_data:
        user_id = str(record[0])
        prosthesis_type = record[1]
        signal_date = record[6].date()  # Дата без времени
        
        key = (user_id, prosthesis_type, signal_date)
        
        if key not in grouped_data:
            grouped_data[key] = {
                'signals': [],
                'frequencies': [],
                'durations': [],
                'amplitudes': [],
                'muscle_groups': set(),
                'hourly_counts': [0] * 24
            }
        
        # Собираем статистику
        data = grouped_data[key]
        data['signals'].append(record)
        data['frequencies'].append(record[3])
        data['durations'].append(record[4])
        data['amplitudes'].append(record[5])
        data['muscle_groups'].add(record[2])
        data['hourly_counts'][record[6].hour] += 1
    
    # Подготавливаем данные для загрузки
    daily_reports = []
    
    for (user_id, prosthesis_type, report_date), stats in grouped_data.items():
        if user_id not in crm_data:
            logger.warning(f"Пользователь {user_id} не найден в CRM, пропускаем")
            continue
        
        user_info = crm_data[user_id]
        
        # Находим час пиковой активности
        peak_activity_hour = stats['hourly_counts'].index(max(stats['hourly_counts']))
        
        # Наиболее часто используемая группа мышц
        muscle_groups_list = [r[2] for r in stats['signals']]
        if muscle_groups_list:
            muscle_group = max(set(muscle_groups_list), key=muscle_groups_list.count)
        else:
            muscle_group = 'unknown'
        
        # Рассчитываем статистику
        avg_frequency = sum(stats['frequencies']) / len(stats['frequencies']) if stats['frequencies'] else 0
        avg_duration = sum(stats['durations']) / len(stats['durations']) if stats['durations'] else 0
        avg_amplitude = sum(stats['amplitudes']) / len(stats['amplitudes']) if stats['amplitudes'] else 0
        
        daily_report = {
            'report_date': report_date,
            'user_id': int(user_id),
            'prosthesis_type': prosthesis_type,
            'muscle_group': muscle_group,
            'total_signals': len(stats['signals']),
            'avg_frequency': avg_frequency,
            'avg_duration': avg_duration,
            'avg_amplitude': avg_amplitude,
            'max_amplitude': max(stats['amplitudes']) if stats['amplitudes'] else 0,
            'min_amplitude': min(stats['amplitudes']) if stats['amplitudes'] else 0,
            'peak_activity_hour': peak_activity_hour,
            'user_name': user_info['name'],
            'user_email': user_info['email'],
            'user_country': user_info['country'],
            'user_age': user_info['age'] if user_info['age'] else 0,
            'user_gender': user_info['gender'] if user_info['gender'] else 'unknown'
        }
        
        daily_reports.append(daily_report)
    
    logger.info(f"Сформировано {len(daily_reports)} ежедневных отчетов")
    context['ti'].xcom_push(key='daily_reports', value=daily_reports)
    return daily_reports

def transform_monthly_data(**context):
    """Трансформация данных для месячного отчета"""
    logger.info("Начало трансформации данных для месячного отчета")
    
    daily_reports = context['ti'].xcom_pull(task_ids='transform_daily_data', key='daily_reports')
    
    if not daily_reports:
        logger.warning("Нет ежедневных отчетов для агрегации")
        context['ti'].xcom_push(key='monthly_reports', value=[])
        return []
    
    # Группируем по месяцу, пользователю и типу протеза
    monthly_groups = {}
    
    for daily in daily_reports:
        # Получаем первый день месяца
        month_start = datetime(daily['report_date'].year, daily['report_date'].month, 1).date()
        
        key = (daily['user_id'], daily['prosthesis_type'], month_start)
        
        if key not in monthly_groups:
            monthly_groups[key] = {
                'reports': [],
                'amplitudes': [],
                'daily_totals': [],
                'active_days': set()
            }
        
        group = monthly_groups[key]
        group['reports'].append(daily)
        group['amplitudes'].append(daily['max_amplitude'])
        group['amplitudes'].append(daily['min_amplitude'])
        group['daily_totals'].append(daily['total_signals'])
        group['active_days'].add(daily['report_date'])
    
    # Формируем месячные отчеты
    monthly_reports = []
    
    for (user_id, prosthesis_type, report_month), stats in monthly_groups.items():
        if not stats['reports']:
            continue
        
        # Берем данные пользователя из первого отчета
        sample_report = stats['reports'][0]
        
        # Находим самый активный день (по количеству сигналов)
        most_active_day = 1
        max_signals = 0
        for report in stats['reports']:
            if report['total_signals'] > max_signals:
                max_signals = report['total_signals']
                most_active_day = report['report_date'].day
        
        # Рассчитываем средние значения
        avg_frequency = sum(r['avg_frequency'] for r in stats['reports']) / len(stats['reports'])
        avg_duration = sum(r['avg_duration'] for r in stats['reports']) / len(stats['reports'])
        avg_amplitude = sum(r['avg_amplitude'] for r in stats['reports']) / len(stats['reports'])
        
        monthly_report = {
            'report_month': report_month,
            'user_id': user_id,
            'prosthesis_type': prosthesis_type,
            'muscle_group': sample_report['muscle_group'],
            'total_signals': sum(stats['daily_totals']),
            'avg_daily_signals': sum(stats['daily_totals']) / len(stats['daily_totals']),
            'avg_frequency': avg_frequency,
            'avg_duration': avg_duration,
            'avg_amplitude': avg_amplitude,
            'max_amplitude_daily': max(stats['amplitudes']) if stats['amplitudes'] else 0,
            'min_amplitude_daily': min(stats['amplitudes']) if stats['amplitudes'] else 0,
            'most_active_day': most_active_day,
            'user_name': sample_report['user_name'],
            'user_email': sample_report['user_email'],
            'user_country': sample_report['user_country'],
            'user_age': sample_report['user_age'],
            'user_gender': sample_report['user_gender'],
            'days_used': len(stats['active_days'])
        }
        
        monthly_reports.append(monthly_report)
    
    logger.info(f"Сформировано {len(monthly_reports)} месячных отчетов")
    context['ti'].xcom_push(key='monthly_reports', value=monthly_reports)
    return monthly_reports

def load_to_clickhouse(**context):
    """Загрузка данных в ClickHouse"""
    logger.info("Начало загрузки данных в ClickHouse")
    
    # Получаем преобразованные данные
    daily_reports = context['ti'].xcom_pull(task_ids='transform_daily_data', key='daily_reports')
    monthly_reports = context['ti'].xcom_pull(task_ids='transform_monthly_data', key='monthly_reports')
    
    # Получаем настройки ClickHouse из переменных окружения
    env_vars = get_env_variables()
    
    # Подключаемся к ClickHouse
    clickhouse_client = ClickHouseClient(
        host=env_vars['CLICKHOUSE_HOST'],
        port=env_vars['CLICKHOUSE_PORT'],
        user=env_vars['CLICKHOUSE_USER'],
        password=env_vars['CLICKHOUSE_PASSWORD'],
        database=env_vars['CLICKHOUSE_DB']
    )
    
    logger.info(f"Подключение к ClickHouse: {env_vars['CLICKHOUSE_HOST']}:{env_vars['CLICKHOUSE_PORT']}")
    
    # Создаем таблицы если они не существуют
    create_tables_queries = [
        """
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
        PARTITION BY toYYYYMM(report_date)
        """,
        """
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
        PARTITION BY toYYYYMM(report_month)
        """
    ]
    
    for query in create_tables_queries:
        try:
            clickhouse_client.execute(query)
            logger.info("Таблица успешно создана или уже существует")
        except Exception as e:
            logger.error(f"Ошибка при создании таблицы: {e}")
    
    # Загружаем ежедневные отчеты
    if daily_reports:
        daily_insert_query = """
        INSERT INTO daily_prosthesis_reports (
            report_date, user_id, prosthesis_type, muscle_group,
            total_signals, avg_frequency, avg_duration, avg_amplitude,
            max_amplitude, min_amplitude, peak_activity_hour,
            user_name, user_email, user_country, user_age, user_gender
        ) VALUES
        """
        
        daily_values = []
        for report in daily_reports:
            daily_values.append((
                report['report_date'],
                report['user_id'],
                report['prosthesis_type'],
                report['muscle_group'],
                report['total_signals'],
                report['avg_frequency'],
                report['avg_duration'],
                report['avg_amplitude'],
                report['max_amplitude'],
                report['min_amplitude'],
                report['peak_activity_hour'],
                report['user_name'],
                report['user_email'],
                report['user_country'],
                report['user_age'],
                report['user_gender']
            ))
        
        try:
            clickhouse_client.execute(daily_insert_query, daily_values)
            logger.info(f"Загружено {len(daily_reports)} ежедневных отчетов")
        except Exception as e:
            logger.error(f"Ошибка при загрузке ежедневных отчетов: {e}")
            raise
    
    # Загружаем месячные отчеты
    if monthly_reports:
        monthly_insert_query = """
        INSERT INTO monthly_prosthesis_reports (
            report_month, user_id, prosthesis_type, muscle_group,
            total_signals, avg_daily_signals, avg_frequency, avg_duration,
            avg_amplitude, max_amplitude_daily, min_amplitude_daily,
            most_active_day, user_name, user_email, user_country,
            user_age, user_gender, days_used
        ) VALUES
        """
        
        monthly_values = []
        for report in monthly_reports:
            monthly_values.append((
                report['report_month'],
                report['user_id'],
                report['prosthesis_type'],
                report['muscle_group'],
                report['total_signals'],
                report['avg_daily_signals'],
                report['avg_frequency'],
                report['avg_duration'],
                report['avg_amplitude'],
                report['max_amplitude_daily'],
                report['min_amplitude_daily'],
                report['most_active_day'],
                report['user_name'],
                report['user_email'],
                report['user_country'],
                report['user_age'],
                report['user_gender'],
                report['days_used']
            ))
        
        try:
            clickhouse_client.execute(monthly_insert_query, monthly_values)
            logger.info(f"Загружено {len(monthly_reports)} месячных отчетов")
        except Exception as e:
            logger.error(f"Ошибка при загрузке месячных отчетов: {e}")
            raise
    
    result_message = f"Загружено: {len(daily_reports) if daily_reports else 0} daily, {len(monthly_reports) if monthly_reports else 0} monthly"
    logger.info(result_message)
    return result_message

def validate_environment(**context):
    """Валидация переменных окружения перед запуском"""
    logger.info("Валидация переменных окружения")
    
    env_vars = get_env_variables()
    
    # Проверяем обязательные переменные
    required_vars = [
        'CRM_DB_HOST', 'CRM_DB_NAME', 'CRM_DB_USER', 'CRM_DB_PASSWORD',
        'TELEMETRY_DB_HOST', 'TELEMETRY_DB_NAME', 'TELEMETRY_DB_USER', 'TELEMETRY_DB_PASSWORD',
        'CLICKHOUSE_HOST', 'CLICKHOUSE_DB'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not env_vars.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info("Все обязательные переменные окружения присутствуют")
    
    # Логируем конфигурацию (без паролей)
    safe_config = {
        'CRM_DB': f"{env_vars['CRM_DB_HOST']}:{env_vars['CRM_DB_PORT']}/{env_vars['CRM_DB_NAME']}",
        'TELEMETRY_DB': f"{env_vars['TELEMETRY_DB_HOST']}:{env_vars['TELEMETRY_DB_PORT']}/{env_vars['TELEMETRY_DB_NAME']}",
        'CLICKHOUSE': f"{env_vars['CLICKHOUSE_HOST']}:{env_vars['CLICKHOUSE_PORT']}/{env_vars['CLICKHOUSE_DB']}"
    }
    
    logger.info(f"Конфигурация: {safe_config}")
    return "Environment validation successful"

# Определение DAG
dag = DAG(
    'bionicpro_reports_etl',
    default_args=default_args,
    description='ETL процесс для генерации отчетов по протезам BionicPRO',
    schedule_interval='0 1 * * *',  # Запуск каждый день в 1:00
    catchup=False,
    tags=['bionicpro', 'reports', 'etl'],
    max_active_runs=1
)

# Определение задач
start_task = EmptyOperator(task_id='start', dag=dag)
end_task = EmptyOperator(task_id='end', dag=dag)

validate_env_task = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag,
    provide_context=True
)

extract_crm_task = PythonOperator(
    task_id='extract_crm_data',
    python_callable=extract_crm_data,
    dag=dag,
    provide_context=True
)

extract_telemetry_task = PythonOperator(
    task_id='extract_telemetry_data',
    python_callable=extract_telemetry_data,
    dag=dag,
    provide_context=True
)

transform_daily_task = PythonOperator(
    task_id='transform_daily_data',
    python_callable=transform_daily_data,
    dag=dag,
    provide_context=True
)

transform_monthly_task = PythonOperator(
    task_id='transform_monthly_data',
    python_callable=transform_monthly_data,
    dag=dag,
    provide_context=True
)

load_clickhouse_task = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    dag=dag,
    provide_context=True
)

# Определение зависимостей
start_task >> validate_env_task
validate_env_task >> [extract_crm_task, extract_telemetry_task]
[extract_crm_task, extract_telemetry_task] >> transform_daily_task
transform_daily_task >> transform_monthly_task
transform_monthly_task >> load_clickhouse_task
load_clickhouse_task >> end_task
import logging

from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)


AWS_CONN_ID = "yandex_s3"
VERTICA_CONN_ID = "vertica_conn"


def load_data_to_stg():
    context = get_current_context()

    logical_date = context["logical_date"]

    # Загружаем данные за предыдущий день
    load_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

    bucket_name = Variable.get("s3_bucket")

    logger.info("Начинаем загрузку данных за дату %s", load_date)

    # Получаем credentials из Airflow Connection
    s3_connection = BaseHook.get_connection(AWS_CONN_ID)

    access_key = s3_connection.login
    secret_key = s3_connection.password

    logger.info("Credentials для S3 получены из Airflow Connection")

    # Файлы с транзакциями
    transaction_files = [f"transactions_batch_{i}.csv" for i in range(1, 11)]

    logger.info("Порядок загрузки transaction-файлов: %s", transaction_files)

    # Подключение к Vertica
    vertica_hook = VerticaHook(vertica_conn_id=VERTICA_CONN_ID)

    conn = vertica_hook.get_conn()
    cursor = conn.cursor()

    logger.info("Подключение к Vertica установлено")

    try:
        # Настраиваем текущую сессию Vertica
        # для работы с Yandex Object Storage
        cursor.execute(f"""
            ALTER SESSION SET AWSAuth =
                '{access_key}:{secret_key}';

            ALTER SESSION SET AWSEndpoint =
                'storage.yandexcloud.net';

            ALTER SESSION SET AWSEnableHttps = 1;
            """)

        logger.info("Настройки Yandex S3 для Vertica установлены")

        # Временная таблица для курсов валют
        cursor.execute("""
            CREATE LOCAL TEMP TABLE tmp_currencies (
                currency_code VARCHAR(3),
                currency_code_with VARCHAR(3),
                date_update DATE,
                currency_with_div NUMERIC(10, 2)
            )
            ON COMMIT PRESERVE ROWS;
            """)

        # Временная таблица для транзакций
        cursor.execute("""
            CREATE LOCAL TEMP TABLE tmp_transactions (
                operation_id UUID,
                account_number_from BIGINT,
                account_number_to BIGINT,
                currency_code VARCHAR(3),
                country VARCHAR(30),
                status VARCHAR(15),
                transaction_type VARCHAR(30),
                amount BIGINT,
                transaction_dt TIMESTAMP
            )
            ON COMMIT PRESERVE ROWS;
            """)

        # Загружаем историю курсов валют
        logger.info("Загружаем currencies_history.csv во временную таблицу")

        cursor.execute(f"""
            COPY tmp_currencies (
                currency_code,
                currency_code_with,
                date_update,
                currency_with_div
            )
            FROM 's3://{bucket_name}/currencies_history.csv'
            DELIMITER ','
            SKIP 1
            NULL AS '';
            """)

        # Последовательно загружаем transaction batches
        for file_name in transaction_files:

            logger.info("Загружаем файл %s во временную таблицу", file_name)

            cursor.execute(f"""
                COPY tmp_transactions (
                    operation_id,
                    account_number_from,
                    account_number_to,
                    currency_code,
                    country,
                    status,
                    transaction_type,
                    amount,
                    transaction_dt
                )
                FROM 's3://{bucket_name}/{file_name}'
                DELIMITER ','
                SKIP 1
                NULL AS '';
                """)

        # Удаляем данные за день,
        # чтобы повторный запуск не создавал дубли
        STAGING_SCHEMA = Variable.get("stg_schema")

        cursor.execute(f"""
            DELETE FROM {STAGING_SCHEMA}.currencies
            WHERE date_update = DATE '{load_date}';
            """)

        # Загружаем курсы только за нужную дату
        cursor.execute(f"""
            INSERT INTO {STAGING_SCHEMA}.currencies (
                currency_code,
                currency_code_with,
                date_update,
                currency_with_div
            )
            SELECT
                currency_code,
                currency_code_with,
                date_update,
                currency_with_div
            FROM tmp_currencies
            WHERE date_update = DATE '{load_date}';
            """)

        # Удаляем транзакции за день,
        # чтобы повторный запуск не создавал дубли
        cursor.execute(f"""
            DELETE FROM {STAGING_SCHEMA}.transactions
            WHERE CAST(transaction_dt AS DATE) = DATE '{load_date}';
            """)

        # Загружаем транзакции только за нужную дату
        cursor.execute(f"""
            INSERT INTO {STAGING_SCHEMA}.transactions (
                operation_id,
                account_number_from,
                account_number_to,
                currency_code,
                country,
                status,
                transaction_type,
                amount,
                transaction_dt
            )
            SELECT
                operation_id,
                account_number_from,
                account_number_to,
                currency_code,
                country,
                status,
                transaction_type,
                amount,
                transaction_dt
            FROM tmp_transactions
            WHERE CAST(transaction_dt AS DATE) = DATE '{load_date}';
            """)

        conn.commit()

        logger.info("Данные за %s успешно загружены в STAGING", load_date)

    except Exception:

        conn.rollback()

        logger.exception("Ошибка загрузки данных за %s", load_date)

        raise

    finally:

        cursor.close()
        conn.close()

        logger.info("Соединение с Vertica закрыто")


with DAG(
    dag_id="1_data_import",
    start_date=datetime(2022, 10, 1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["staging", "s3", "vertica"],
) as dag:

    load_data = PythonOperator(
        task_id="load_data_to_stg",
        python_callable=load_data_to_stg,
    )


load_data

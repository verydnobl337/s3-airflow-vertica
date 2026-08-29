import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.models import Variable

logger = logging.getLogger(__name__)


VERTICA_CONN_ID = "vertica_conn"


def load_global_metrics():
    context = get_current_context()
    logical_date = context["logical_date"]

    # Загружаю данные за предыдущий день
    load_date = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("Начинаем расчет витрины global_metrics за дату %s", load_date)

    vertica_hook = VerticaHook(vertica_conn_id=VERTICA_CONN_ID)

    conn = vertica_hook.get_conn()
    cursor = conn.cursor()

    logger.info("Подключение к Vertica установлено")

    try:
        # Удаляю данные за дату расчета (идемпотентная загрузка):
        DWH_SCHEMA = Variable.get("dwh_schema")
        STAGING_SCHEMA = Variable.get("stg_schema")

        cursor.execute(f"""
            DELETE FROM {DWH_SCHEMA}.global_metrics
            WHERE date_update = DATE '{load_date}';
        """)

        logger.info("Удалены существующие данные global_metrics за %s", load_date)

        # Рассчитываю и загружаю витрину
        cursor.execute(f"""
            INSERT INTO {DWH_SCHEMA}.global_metrics(
                date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions
            )
            SELECT
                DATE(t.transaction_dt) as date_update,
                t.currency_code as currency_from,
                SUM(
                    CASE
                        WHEN t.currency_code = 420 THEN t.amount
                        ELSE t.amount * c.currency_with_div
                    END
                ) as amount_total,
                COUNT(*) as cnt_transactions,
                ROUND(
                    COUNT(*)::NUMERIC
                    / COUNT(DISTINCT t.account_number_from),
                    2
                ) as avg_transactions_per_account,
                COUNT(DISTINCT t.account_number_from) as cnt_accounts_make_transactions
            FROM {STAGING_SCHEMA}.transactions t
            LEFT JOIN {STAGING_SCHEMA}.currencies c
                ON t.currency_code = c.currency_code
                AND c.currency_code_with = 420
                AND c.date_update = DATE(t.transaction_dt)
            WHERE DATE(t.transaction_dt) = DATE '{load_date}'
                AND t.account_number_from >= 0
                AND t.status = 'done' 
            GROUP BY
                DATE(t.transaction_dt),
                t.currency_code;
        """)

        conn.commit()

        logger.info("Витрина global_metrics за %s успешно загружена", load_date)
    except Exception:
        conn.rollback()

        logger.exception("Ошибка расчета global_metrics за %s", load_date)

        raise

    finally:
        cursor.close()
        conn.close()

        logger.info("Соединение с Vertica закрыто")


with DAG(
    dag_id="2_global_metrics",
    start_date=datetime(2022, 10, 2),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["dwh", "global_metrics", "vertica"],
) as dag:

    load_metrics = PythonOperator(
        task_id="load_global_metrics",
        python_callable=load_global_metrics,
    )

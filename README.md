# S3 → Apache Airflow → Vertica

> **Data Engineering portfolio project**
>
> ETL/ELT-пайплайн для ежедневной загрузки транзакционных данных из объектного хранилища S3 в Vertica с оркестрацией Apache Airflow и последующим расчётом аналитической витрины.

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE?logo=apacheairflow&logoColor=white)
![Vertica](https://img.shields.io/badge/Vertica-DWH-5B2C83)
![S3](https://img.shields.io/badge/Object%20Storage-S3-569A31?logo=amazons3&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-ETL-336791)

---

## 1. Назначение проекта

Проект демонстрирует построение типового batch-пайплайна Data Engineer для финансового домена.

Пайплайн решает две основные задачи:

1. **Data Ingestion / STAGING** — загрузка данных о транзакциях и курсах валют из S3 в слой `STAGING` Vertica.
2. **Data Mart / DWH** — расчёт ежедневной аналитической витрины `global_metrics` в слое `DWH`.

Основной акцент проекта — практическое применение **Apache Airflow, S3, Vertica, SQL, идемпотентных загрузок и слоистой архитектуры DWH**.

---

## 2. Архитектура решения

```text
                    ┌──────────────────────────┐
                    │        S3 Object          │
                    │          Storage         │
                    │                          │
                    │ currencies_history.csv   │
                    │ transactions_batch_*.csv │
                    └────────────┬─────────────┘
                                 │
                                 │ COPY FROM S3
                                 ▼
                    ┌──────────────────────────┐
                    │      Apache Airflow      │
                    │                          │
                    │  DAG: 1_data_import      │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │         Vertica          │
                    │                          │
                    │       STAGING           │
                    │  ┌───────────────────┐   │
                    │  │ transactions      │   │
                    │  │ currencies        │   │
                    │  └───────────────────┘   │
                    └────────────┬─────────────┘
                                 │
                                 │ SQL aggregation
                                 ▼
                    ┌──────────────────────────┐
                    │      Apache Airflow      │
                    │                          │
                    │  DAG: 2_global_metrics   │
                    └────────────┬─────────────┘
                                 │
                                 ▼
                    ┌──────────────────────────┐
                    │         Vertica          │
                    │                          │
                    │          DWH             │
                    │  ┌───────────────────┐   │
                    │  │ global_metrics    │   │
                    │  └───────────────────┘   │
                    └──────────────────────────┘
```

### Логические слои

| Слой | Назначение | Объекты |
|---|---|---|
| **Source** | Исходные файлы | S3 CSV |
| **STAGING** | Приём и первичная обработка данных | `transactions`, `currencies` |
| **DWH/CDM** | Подготовленная аналитическая информация | `global_metrics` |

---

## 3. Технологический стек

- **Python** — реализация orchestration logic и работы с Airflow API.
- **Apache Airflow** — оркестрация и расписание batch-процессов.
- **S3 / Yandex Object Storage** — объектное хранилище исходных CSV-файлов.
- **Vertica** — аналитическое хранилище данных.
- **SQL** — DDL, загрузка и агрегация данных.
- **Airflow Connections / Variables** — хранение параметров подключения и конфигурации без размещения секретов в коде.

---

## 4. Структура репозитория

```text
s3-airflow-vertica/
├── README.md
├── docs/
│   └── architecture.md
└── src/
    ├── dags/
    │   ├── 1_data_import.py
    │   └── 2_datamart_update.py
    │
    ├── sql/
    │   ├── stg/
    │   │   ├── DDL_currencies.sql
    │   │   └── DDL_transactions.sql
    │   │
    │   └── cdm/
    │       ├── DDL_global_metrics.sql
    │       └── DML_global_metrics.sql
    │
    ├── img/
    └── py/
```

### Назначение основных файлов

| Файл | Назначение |
|---|---|
| `1_data_import.py` | Ежедневная загрузка S3 → Vertica STAGING |
| `2_datamart_update.py` | Ежедневный расчёт витрины `global_metrics` |
| `DDL_currencies.sql` | DDL таблицы курсов валют и projection |
| `DDL_transactions.sql` | DDL таблицы транзакций и projection |
| `DDL_global_metrics.sql` | DDL аналитической витрины и projection |
| `DML_global_metrics.sql` | SQL расчёта показателей витрины |

---

## 5. Описание ETL-процесса

### 5.1. DAG `1_data_import`

DAG запускается ежедневно и обрабатывает данные за предыдущий календарный день.

Основные этапы:

1. Получение `logical_date` из контекста Airflow.
2. Расчёт даты загрузки: `logical_date - 1 день`.
3. Получение S3 credentials из Airflow Connection.
4. Получение имени bucket из Airflow Variable.
5. Установка параметров доступа Vertica к S3.
6. Создание временных таблиц для исходных данных.
7. Загрузка `currencies_history.csv` во временную таблицу.
8. Последовательная загрузка `transactions_batch_1.csv` … `transactions_batch_10.csv`.
9. Удаление существующих записей за дату из STAGING.
10. Вставка данных только за требуемую дату.
11. `COMMIT` при успешной обработке или `ROLLBACK` при ошибке.

### 5.2. DAG `2_global_metrics`

DAG рассчитывает витрину `global_metrics` за предыдущий день.

Расчёт включает:

- общую сумму транзакций;
- количество транзакций;
- среднее количество транзакций на счёт;
- количество счетов, совершивших транзакции;
- пересчёт сумм в целевую валюту по таблице курсов;
- фильтрацию успешных транзакций со статусом `done`.

Перед расчётом данные за соответствующую дату удаляются из витрины. Это делает загрузку **идемпотентной**: повторный запуск за тот же день не должен создавать дубликаты.

---

## 6. Модель данных

### STAGING: `transactions`

| Поле | Тип | Описание |
|---|---|---|
| `operation_id` | UUID | Идентификатор операции |
| `account_number_from` | BIGINT | Счёт отправителя |
| `account_number_to` | BIGINT | Счёт получателя |
| `currency_code` | VARCHAR(3) | Валюта операции |
| `country` | VARCHAR(30) | Страна |
| `status` | VARCHAR(15) | Статус операции |
| `transaction_type` | VARCHAR(30) | Тип операции |
| `amount` | BIGINT | Сумма операции |
| `transaction_dt` | TIMESTAMP | Дата и время операции |

### STAGING: `currencies`

| Поле | Тип | Описание |
|---|---|---|
| `currency_code` | VARCHAR(3) | Исходная валюта |
| `currency_code_with` | VARCHAR(3) | Целевая валюта |
| `date_update` | DATE | Дата курса |
| `currency_with_div` | NUMERIC(10,2) | Коэффициент пересчёта |

### DWH: `global_metrics`

| Поле | Тип | Описание |
|---|---|---|
| `date_update` | DATE | Дата расчёта |
| `currency_from` | VARCHAR(3) | Исходная валюта |
| `amount_total` | NUMERIC(18,2) | Общая сумма |
| `cnt_transactions` | INTEGER | Количество транзакций |
| `avg_transactions_per_account` | NUMERIC(10,2) | Среднее число транзакций на счёт |
| `cnt_accounts_make_transactions` | INTEGER | Количество активных счетов |

---

## 7. Идемпотентность и обработка ошибок

В проекте реализована повторяемая загрузка за конкретную дату:

```text
START
  │
  ├── DELETE records for load_date
  │
  ├── INSERT records for load_date
  │
  └── COMMIT

При ошибке
  │
  └── ROLLBACK
```

Это позволяет повторно запускать DAG после технического сбоя без накопления дублей за одну и ту же дату.

Соединения с Vertica закрываются в блоке `finally`, а ошибки логируются через стандартный Python `logging`.

---

## 8. Конфигурация Airflow

Проект не содержит credentials в исходном коде. Для работы необходимо настроить следующие объекты Airflow.

### Connections

| Connection ID | Тип | Назначение |
|---|---|---|
| `yandex_s3` | Amazon Web Services | Доступ к S3/Object Storage |
| `vertica_conn` | Vertica | Подключение к Vertica |

### Variables

| Variable | Назначение |
|---|---|
| `s3_bucket` | Имя S3 bucket |
| `stg_schema` | Схема STAGING |
| `dwh_schema` | Схема DWH |

Для production-среды credentials рекомендуется передавать через защищённое хранилище секретов или secrets backend Airflow.

---

## 9. Расписание DAG

### `1_data_import`

```text
schedule = @daily
catchup   = True
max_active_runs = 1
```

### `2_global_metrics`

```text
schedule = @daily
catchup   = True
max_active_runs = 1
```

Оба DAG используют `logical_date` и обрабатывают данные за предыдущий календарный день.

---

## 10. Запуск проекта

### Шаг 1. Подготовить инфраструктуру

Необходимо иметь:

- работающий Apache Airflow;
- доступ к S3-compatible Object Storage;
- доступ к Vertica;
- установленный провайдер Airflow для Vertica.

### Шаг 2. Создать структуры Vertica

Выполнить DDL в следующем порядке:

```text
src/sql/stg/DDL_currencies.sql
src/sql/stg/DDL_transactions.sql
src/sql/cdm/DDL_global_metrics.sql
```

### Шаг 3. Настроить Airflow

Создать Connections и Variables из раздела **8. Конфигурация Airflow**.

### Шаг 4. Разместить DAG-файлы

Скопировать содержимое `src/dags/` в директорию DAGs вашего Airflow.

### Шаг 5. Запустить DAGs

Сначала:

```text
1_data_import
```

затем:

```text
2_global_metrics
```

Для исторической загрузки предусмотрен `catchup=True`.

---

## 11. Контроль качества данных

В процессе загрузки реализованы базовые защитные механизмы:

- загрузка только требуемой даты;
- повторяемая загрузка без накопления дублей;
- транзакционная фиксация через `COMMIT` / `ROLLBACK`;
- использование справочника курсов валют по дате операции;
- фильтрация успешных операций при построении витрины;
- исключение некорректных значений `account_number_from < 0` из аналитического расчёта.

---

## 12. Особенности реализации

### Прямой `COPY` из S3 в Vertica

Вместо загрузки файлов через Python используется нативный механизм Vertica `COPY FROM S3`. Это уменьшает количество промежуточных преобразований и позволяет выполнять ingestion непосредственно средствами аналитической СУБД.

### Временные таблицы

Для каждого запуска создаются локальные временные таблицы, куда сначала попадают исходные данные. После этого выполняется фильтрация по `load_date` и перенос только необходимого среза в STAGING.

### Vertica projections

Для таблиц STAGING и DWH определены projections с сортировкой по датам. Для таблицы транзакций используется сегментация по `HASH(transaction_dt, operation_id)`.

---

## 13. Результат проекта

В результате построен воспроизводимый batch ETL/ELT-процесс:

```text
S3
 ↓
Apache Airflow
 ↓
Vertica STAGING
 ↓
SQL transformation
 ↓
Vertica DWH / CDM
 ↓
global_metrics
```

Проект демонстрирует навыки, релевантные позиции **Junior Data Engineer / Data Engineer**:

- разработка ETL/ELT-пайплайнов;
- Apache Airflow DAGs;
- работа с object storage S3;
- SQL и DDL/DML;
- Vertica и аналитические хранилища;
- DWH layers: STAGING → DWH/CDM;
- идемпотентные загрузки;
- транзакционность и обработка ошибок;
- работа с Airflow Connections и Variables;
- проектирование projections в Vertica.

---

## 14. Ограничения текущей версии

Проект ориентирован на демонстрацию batch-пайплайна и не является production-ready платформой целиком. В частности, в текущей версии отсутствуют отдельные компоненты для:

- автоматизированного мониторинга SLA;
- централизованного Data Quality framework;
- алертинга;
- CI/CD;
- автоматических unit/integration tests;
- schema registry и streaming ingestion.

Эти компоненты являются естественными направлениями дальнейшего развития решения.

---

## 15. Автор и репозиторий

**GitHub:** [verydnobl337/s3-airflow-vertica](https://github.com/verydnobl337/s3-airflow-vertica)

Проект подготовлен как самостоятельная практическая работа по разработке ETL/ELT-пайплайна и предназначен для демонстрации инженерных навыков в портфолио Data Engineer.

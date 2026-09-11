# Архитектура решения

## Поток данных

```text
S3 Object Storage
    │
    │ COPY FROM S3
    ▼
Airflow DAG: 1_data_import
    │
    ▼
Vertica STAGING
    ├── transactions
    └── currencies
    │
    │ SQL aggregation + currency conversion
    ▼
Airflow DAG: 2_global_metrics
    │
    ▼
Vertica DWH/CDM
    └── global_metrics
```

## Принцип обработки даты

Оба DAG получают `logical_date` из контекста Airflow и рассчитывают дату загрузки как предыдущий календарный день.

```text
load_date = logical_date - 1 day
```

Такой подход позволяет использовать `catchup` для исторической загрузки и повторно рассчитывать отдельный дневной срез.

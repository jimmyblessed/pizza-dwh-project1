# Pizza DWH Project 🍕

Data Engineering проект: полный пайплайн от сбора данных до аналитики.

## Стек технологий

- **Python** — генератор данных, ETL
- **PostgreSQL** — OLTP, модель "Звезда"
- **Apache Airflow** — оркестрация пайплайнов
- **Apache Kafka** — стриминг данных
- **Apache Spark** — Big Data обработка
- **ClickHouse** — OLAP для аналитики
- **Docker** — контейнеризация

## Архитектура

[Генератор] → [Kafka] → [Consumer] → [PostgreSQL]
                                          ↓
                                     [Airflow]
                                          ↓
                                     [Spark]
                                          ↓
                                     [ClickHouse]
                                          ↓
                                       [Дашборды]

## Что умеет

- Генерирует заказы в реальном времени
- Буферизует через Kafka (DLQ для ошибок)
- Хранит сырые данные в PostgreSQL (OLTP)
- Строит модель "Звезда" через Airflow
- Обрабатывает Big Data через Spark
- Агрегирует в ClickHouse (OLAP)
- Визуализирует через Superset

## Как запустить

```bash
docker-compose up -d

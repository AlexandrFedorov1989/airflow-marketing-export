# airflow-marketing-export

ТЗ: интеграция с внешним REST API (marketing events).

Сделал свой Hook, Operators, Sensor и DAG.

**Стек на VM:** Ubuntu 22.04, Airflow 2.9.3, PostgreSQL, LocalExecutor.  
**Mock API:** FastAPI на порту 8081 — Airflow ходит к нему обычным HTTP.

---

## Структура репозитория

```text
dags/                          # DAG'и (full и incremental)
plugins/marketing_api/         # Hook, Operators, Sensor, утилиты
mock_api/                      # фейковый REST API
screenshots/                   # скрины UI и логов для сдачи
tests/                         # пара тестов на resolve()
requirements.txt
README.md
ANSWERS.md                     # ответы / пояснения
```

---

## Как устроено

```mermaid
flowchart TB
    subgraph vm [VM Ubuntu 22.04]
        subgraph airflow [Airflow 2.9.3 LocalExecutor]
            Web[Webserver :8080]
            Sched[Scheduler]
            DAG[daily_marketing_export]
        end
        subgraph pg [PostgreSQL]
            MetaDB[(airflow)]
            StateDB[(marketing_state)]
        end
        Mock[Mock REST API :8081]
        Data[data/raw/marketing_events/]
        Plugins[plugins/marketing_api/]
    end
    User[Пользователь] --> Web
    Sched --> DAG
    DAG --> Plugins
    Plugins -->|HTTP REST| Mock
    Sched --> MetaDB
    DAG --> Data
    Plugins --> StateDB
```



### Цепочка задач

В обоих DAG одна и та же цепочка:

1. `validate_connection` — проверка что API живой
2. `start_export` — старт выгрузки, `job_id` в XCom
3. `wait_export_ready` — Sensor ждёт `completed`
4. `download_result` — скачиваем JSONL
5. `verify_file` — проверяем файл
6. `write_success_marker` — пишем `_SUCCESS`
7. `update_last_successful_ts` — сохраняем watermark для incremental

### Кто за что отвечает


| Компонент | Что делает                         |
| --------- | ---------------------------------- |
| Hook      | HTTP, ретраи, auth, JSON           |
| Operator  | бизнес-шаги (старт / скачать)      |
| Sensor    | ждёт пока job станет ready         |
| DAG       | расписание, retries, порядок задач |


---

## Инструкция запуска

Все команды — **на VM**, где крутится Airflow.

```bash
git clone <your-repo-url> ~/airflow-marketing-export
```

### 1. Системные пакеты

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv python3-dev \
  build-essential libssl-dev libffi-dev libpq-dev postgresql postgresql-contrib \
  tmux curl git
```

### 2. PostgreSQL

```bash
sudo -u postgres psql <<'SQL'
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER marketing WITH PASSWORD 'marketing';
CREATE DATABASE marketing_state OWNER marketing;
SQL
```

- `airflow` — метаданные Airflow
- `marketing_state` — `last_successful_ts` для incremental

### 3. Airflow 2.9.3

```bash
python3 -m venv ~/airflow_venv
source ~/airflow_venv/bin/activate
pip install --upgrade pip setuptools wheel

AIRFLOW_VERSION="2.9.3"
PY="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"

pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install -r ~/airflow-marketing-export/requirements.txt

export AIRFLOW_HOME=~/airflow
mkdir -p ~/airflow/dags ~/airflow/plugins ~/airflow/logs ~/airflow/data/raw/marketing_events
```

В `~/airflow/airflow.cfg`:

```ini
[core]
executor = LocalExecutor
load_examples = False

[database]
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
```

```bash
airflow db migrate
airflow users create --username admin --password admin \
  --firstname Air --lastname Flow --role Admin --email admin@example.com
```

В `~/.bashrc`:

```bash
export AIRFLOW_HOME=~/airflow
export PATH="$HOME/airflow_venv/bin:$PATH"
export MARKETING_STATE_PG_URI="postgresql://marketing:marketing@localhost:5432/marketing_state"
```

### 4. Подключение кода к Airflow

```bash
ln -sfn ~/airflow-marketing-export/dags/daily_marketing_export.py ~/airflow/dags/
ln -sfn ~/airflow-marketing-export/dags/daily_marketing_export_incremental.py ~/airflow/dags/
ln -sfn ~/airflow-marketing-export/plugins/marketing_api ~/airflow/plugins/marketing_api
```

### 5. Mock REST API (FastAPI)

```bash
cd ~/airflow-marketing-export/mock_api
source ~/airflow_venv/bin/activate
uvicorn app:app --host 127.0.0.1 --port 8081
```

Проверка: `curl http://127.0.0.1:8081/health` → `{"status":"ok"}`


| Метод | URL                                 | Зачем             |
| ----- | ----------------------------------- | ----------------- |
| GET   | `/health`                           | healthcheck       |
| POST  | `/api/v1/exports`                   | старт → `job_id`  |
| GET   | `/api/v1/exports/{job_id}`          | статус            |
| GET   | `/api/v1/exports/{job_id}/download` | скачать результат |


### 6. Запуск Airflow (tmux)

```bash
tmux new -s airflow
# окно 0
source ~/airflow_venv/bin/activate && export AIRFLOW_HOME=~/airflow
airflow webserver --port 8080

# Ctrl+B, C — окно scheduler
airflow scheduler

# Ctrl+B, C — окно mock-api (если ещё не запущен)
cd ~/airflow-marketing-export/mock_api && uvicorn app:app --host 127.0.0.1 --port 8081
```

UI: `http://<IP_VM>:8080`

### 7. Airflow Connection

Admin → Connections → создать `marketing_api_default`:


| Поле            | Значение                |
| --------------- | ----------------------- |
| Connection Id   | `marketing_api_default` |
| Connection Type | HTTP                    |
| Host            | `127.0.0.1`             |
| Port            | `8081`                  |
| Schema          | `http`                  |


Extra:

```json
{
  "api_version": "v1",
  "timeout": 30,
  "poll_interval": 30,
  "default_format": "jsonl",
  "verify_ssl": true,
  "max_page_size": 1000
}
```

### 8. Запуск DAG

1. Включить DAG `daily_marketing_export`
2. Trigger DAG
3. Проверить результат:

```bash
ls ~/airflow/data/raw/marketing_events/dt=<DATE>/
# export.jsonl  _SUCCESS
```

`catchup=False` — при включении не нагоняет старые даты с start_date.

---

## Sensor: почему reschedule


| mode           | Что делает                      | Беру?  |
| -------------- | ------------------------------- | ------ |
| poke           | воркер сидит всё время ожидания | нет    |
| **reschedule** | между опросами слот свободен    | **да** |
| deferrable     | нужен triggerer                 | нет    |


Выгрузка async и может идти долго. Sensor без своего state в памяти (`job_id` из XCom) — reschedule подходит.

---

## Приоритет конфига

См. `plugins/marketing_api/utils/config_resolver.py`

1. **параметр в Operator** — самый важный
2. **Connection Extra** — дефолты среды
3. **дефолт в коде** — чтобы не падать без Extra

Логика простая: то, что явно написал в задаче, важнее общего Extra. Extra позволяет крутить timeout/poll без правки DAG.

---

## Full и incremental


| Режим           | DAG                                  | Суть                                     |
| --------------- | ------------------------------------ | ---------------------------------------- |
| **full**        | `daily_marketing_export`             | вся выгрузка за период                   |
| **incremental** | `daily_marketing_export_incremental` | только `updated_at > last_successful_ts` |


Incremental хранит timestamp в Postgres `marketing_state`.  
Файлы: `export.jsonl` и `export_incremental.jsonl` в `dt={{ ds }}/`.  
Повторный запуск: пишу через `.tmp` → rename, файл за дату просто перезаписывается.

---

## Ошибки


| Что случилось       | Где ловим         | Что делаем                |
| ------------------- | ----------------- | ------------------------- |
| HTTP 429            | Hook              | retry + backoff           |
| HTTP 500            | Hook              | retry, потом fail         |
| Timeout             | Hook              | retry, потом fail         |
| Пустой / битый JSON | Hook              | fail с нормальным текстом |
| Job failed          | Sensor            | падает task               |
| Пустой файл         | Operator / verify | fail, без `_SUCCESS`      |
| Временные сбои      | DAG               | retries=2, delay 5 min    |


---

## Логи

**Пишу в лог:** параметры, conn_id, job_id, HTTP method/URL/status/время, путь и размер файла.

**Не пишу:** password, token, секреты Connection, личные данные из events.

---

## Скриншоты для сдачи


| Файл                            | Что на нём               |
| ------------------------------- | ------------------------ |
| `screenshots/graph_success.png` | успешный Graph           |
| `screenshots/connection.png`    | Connection без секретов  |
| `screenshots/run_failed.png`    | failed run               |
| `screenshots/log_success.png`   | лог `start_export`       |
| `screenshots/log_failed.png`    | лог ошибки (healthcheck) |


Подробные ответы — в `[ANSWERS.md](ANSWERS.md)`.
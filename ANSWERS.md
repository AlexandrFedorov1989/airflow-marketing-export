# Пояснения к ДЗ (Airflow + marketing API)

Ниже ответы своими словами — что сделал и почему так.

---

## 1. Зачем Hook, а не requests прямо в Operator?

Всё сетевое сложил в Hook: URL, авторизация, ретраи на 429/500, timeout, разбор JSON.

Operator только говорит «стартни выгрузку» / «скачай файл». Так проще: Sensor тоже ходит через тот же Hook, и если API поменяется — правки в одном месте.

---

## 2. Почему Operator не крутит цикл ожидания?

Потому что ждать async-выгрузку должен Sensor. Operator стартует job, кладёт `job_id` в XCom и заканчивается.

Если крутить while True внутри Operator:
- слот воркера занят зря
- смешиваются «запуск» и «ожидание»
- сложнее смотреть в UI, что именно упало

---

## 3. Что делает Sensor?

`MarketingExportSensor` раз в `poke_interval` смотрит статус job по `job_id` из XCom.

- `completed` → True (идём дальше)
- `pending` / `running` → False (ещё ждём)
- `failed` / `cancelled` → сразу ошибка

Стоят timeout, poke_interval и mode=reschedule.

---

## 4. Почему mode=reschedule, а не poke?

При poke воркер сидит и ждёт между опросами. Выгрузка может идти минуты — жалко слот.

При reschedule между poke задача снимается, слот свободен. У меня Sensor без своего состояния в памяти: `job_id` каждый раз из XCom, так что reschedule нормально работает.

deferrable не брал — нужен triggerer, для одной VM это лишнее.

---

## 5. Зачем Connection, почему не хардкодить URL и token?

Host, порт, логин/token и Extra лежат в Connection. В коде только `conn_id`.

Так можно поменять стенд (dev/prod) без правки DAG и не светить секреты в git.

---

## 6. Что в Extra?

Параметры среды, не секреты:
`api_version`, `timeout`, `poll_interval`, `default_format`, `verify_ssl`, `max_page_size`.

Hook/Sensor читают их через `extra_dejson` и `resolve()`.

---

## 7. Почему приоритет: Operator → Extra → default?

1. Явно передали в Operator — это задумано для этой задачи, берём это.
2. Нет — смотрим Extra connection (дефолты окружения).
3. Нет и там — безопасный дефолт в коде, чтобы DAG не падал из‑за пустого Extra.

Так можно подкрутить timeout на одной задаче, не трогая Connection, и наоборот — поменять Extra без деплоя DAG.

---

## 8. Где обрабатываются 429, 500, timeout, кривой JSON?

Всё в Hook, метод `_request`:
- 429 — retry + backoff
- 500 — retry, потом raise
- timeout — retry, потом ошибка
- пустой body / битый JSON — сразу понятная ошибка

Operator и Sensor HTTP сами не пишут.

---

## 9. Failed job и пустой файл?

- Sensor: статус failed/cancelled → task падает
- DownloadOperator + verify_file: пустой файл / битый JSONL → ошибка, `_SUCCESS` не пишется
- в DAG: retries=2, retry_delay=5min на временные сбои

---

## 10. Зачем XCom?

Передаю мелочь между тасками:
- `job_id` — от start_export к Sensor и download
- `output_path`, `file_size` — дальше по цепочке

Большие файлы в XCom не кладу — только id и пути.

---

## 11. Full vs incremental

| | full | incremental |
|---|---|---|
| что качаем | весь период date_from–date_to | только updated_at > last_successful_ts |
| state | не обязателен | читаю/пишу StateStore в PostgreSQL |
| параметр API | mode=full | mode=incremental + updated_after |

Два DAG:
- `daily_marketing_export` — full
- `daily_marketing_export_incremental` — incremental

---

## 12. Идемпотентность при повторном запуске за ту же дату

Путь один и тот же: `data/raw/marketing_events/dt={{ ds }}/export.jsonl`.

Скачиваю во временный `.tmp`, потом `os.replace`. Повторный run просто перезапишет файл за эту дату, дублей файлов не будет.

---

## 13. Две базы Postgres?

- `airflow` — метаданные Airflow (runs, connections и т.д.)
- `marketing_state` — мой watermark для incremental (`last_successful_ts`)

Не хотел мешать служебные таблицы Airflow и свой стейт.

---

## 14. Почему LocalExecutor?

Sequential — одна задача за раз, Sensor тормозит всё.
LocalExecutor — параллельно на одной VM, для ДЗ нормально.
Celery — нужен брокер и воркеры, для одной машины избыточно.

---

## 15. Почему catchup=False?

Чтобы при первом включении DAG не нагнал кучу старых дней с start_date. Обычные daily-прогоны «с сегодня» хватает; backfill при необходимости можно руками из UI.

---

## 16. Что логирую / что нет

Логирую: старт таски, даты/mode/path, conn_id, job_id, HTTP method/URL/status/время/attempt, путь и размер файла.

Не логирую: password, token, Bearer, секреты из Connection, персональные данные из events.

---

## 17. Зачем mock на FastAPI?

Нужен API с job_id + polling + download. Публичные демо-API так обычно не умеют.

FastAPI — просто mock-сервер в `mock_api/`. Airflow ходит к нему по HTTP как к обычному сервису. Плюс можно симулировать 429/500/timeout.

---

## 18. Почему plugin, а не один файл?

Airflow сам подхватывает `plugins/`. Разложил на hooks / operators / sensors / utils — так проще читать и править. Один файл на 500+ строк по заданию как раз не ок.

---

## 19. Как показать failed run?

Остановить mock API и Trigger DAG — упадёт `validate_connection` (healthcheck).
Либо в mock есть `?simulate=429|500|timeout|empty|job_failed`.

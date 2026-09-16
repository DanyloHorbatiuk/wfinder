# SPEC — WorkFinder: розширення ETL-пайплайна

Технічна специфікація для реалізації в Claude Code. Порядок і статус робіт описано в `ROADMAP.md`, правила роботи — у `CLAUDE.md`.
Код, ідентифікатори, коментарі й повідомлення комітів пишуться англійською. Тексти для користувача (Telegram) і ці документи — українською.

---

## 1. Контекст

### 1.1 Мета

Перетворити наявний ETL-пайплайн збору ІТ-курсів і стажувань на систему моніторингу, яка:

- коректно веде історію змін кожної пропозиції;
- не псує історію, коли джерело дає збій;
- повідомляє про збої;
- витягує з пропозицій затребувані технології;
- показує аналітику в Metabase.

Робочі варіанти теми:

- «Автоматизована система моніторингу ринку ІТ-стажувань із версіонуванням даних та аналізом затребуваних технологій»
- «ETL-пайплайн з історизацією (SCD Type 2) і контролем якості даних для аналізу пропозицій ІТ-стажувань»

### 1.2 Поза обсягом

Рекомендації (TF-IDF, ембеддинги, pgvector), інтерактивний Telegram-бот, REST API, dbt та окремий фреймворк шарів bronze/silver/gold, CI, Makefile і тестова інфраструктура, оформлення звіту з практики. Нові джерела даних не заплановані, але допускаються за шаблоном адаптера.

---

## 2. Поточний стан (за кодом, коміт `b0997c3`)

DAG `load_courses_pipeline_dag` (`dags/load_courses_dag.py`) запускається за розкладом `30 8 * * *` UTC і складається з таких задач:

`get_sources` (змінні `CAREERS_URL_*`) → `fetch_one_source.expand()` (curl_cffi → MinIO `raw/{source}_{timestamp}.json` + рядок `file_record`) → `load_all_to_db` (файли `pending` → адаптер → `CourseRepository.upsert_course`) → `notify_new_courses_task` (Telegram-дайджест).

| Шлях | Роль |
|---|---|
| `adapters/` | `BaseAdapter`, `EpamAdapter`, `SoftServeAdapter` |
| `core/db.py` | engine, `Session`, `init_db()` / `reset_db()` через `create_all` |
| `core/model.py` | `FileRecord`, `Course` |
| `core/repository.py` | `FileRecordRepository`, `CourseRepository` |
| `core/setting.py` | pydantic-settings; `.env` читається з кореня проєкту (`BASE_DIR` = батьківська тека `core/`) |
| `storage/minio.py` | клієнт boto3, `save_object` |
| `storage/loader.py` | `process_pending_files`, `save_file_record` |
| `notify/telegram.py`, `notify/digest.py` | надсилання та дайджест |
| `notify/courses.py` | мертвий дублікат дайджесту |
| `fetch.py`, `main.py` | завантаження; локальний запуск (зламаний) |

Сервіси Docker Compose:

- `postgres` (17, дані);
- `pgadmin`;
- `minio`;
- `metabase` (v0.53.4, внутрішня БД у тому самому Postgres, база `MB_DB_DBNAME`);
- `postgres-airflow`, `airflow-init`, `airflow-dag-processor`;
- `airflow-webserver` (команда `api-server`, порт `:8081`);
- `airflow-scheduler`: збирається з `Dockerfile`, використовує LocalExecutor, тобто задачі виконуються саме тут; проєкт змонтовано в `/opt/airflow/project`, цей шлях є в `PYTHONPATH`.

Схема БД створюється через `create_all`, міграцій немає.

---

## 3. Відомі дефекти

| ID | Де | Проблема | Наслідок | Виправляє |
|---|---|---|---|---|
| D1 | `core/repository.py` `upsert_course` | Завжди закриває активну версію й вставляє нову, навіть без змін; `EXCLUDED_FIELDS` не використовується | Щодня створюється нова версія кожного запису, історія непридатна для аналізу | F-02 |
| D2 | `core/repository.py` `get_courses_created_today` | `created_at` нової версії завжди «сьогодні» | Дайджест щодня подає всі активні записи як нові | F-02, E-07 |
| D3 | логіка upsert | Записи, що зникли з джерела, ніколи не закриваються | Неможливо рахувати тривалість життя пропозицій | E-06 |
| D4 | `core/model.py` | `Index(...)` у тілі класу без `__table_args__` не прив'язаний до таблиці | Частковий унікальний індекс, імовірно, не створено; дублікати активних версій не блокуються | F-01 |
| D5 | `fetch.py` | `fetch_and_save_single` і `load_file_and_meta` перехоплюють винятки й повертають `status=error` | `retries=3` не спрацьовують, задача вважається успішною, збої невидимі | F-03 |
| D6 | `storage/loader.py` `process_one_file` | Немає обробки винятків; статус `processing` комітиться окремо | Один зламаний файл зупиняє обробку решти й назавжди лишається в `processing` | F-03 |
| D7 | `storage/loader.py` | Порядок обробки файлів не визначений; `active_from` = час обробки, а не час знімка | Неправильні дати в історії, коректний replay неможливий | F-02 |
| D8 | `fetch.py` | Мітка часу в ключі — локальний час із суфіксом `Z` | Неоднозначний час знімка | F-02 |
| D9 | `notify/courses.py` | `notify_new_courses()` викликається на рівні модуля | Повідомлення надсилається при імпорті | F-01 |
| D10 | `main.py` | Імпорт неіснуючої `get_data_from_resources` | Скрипт не запускається | F-01 |
| D11 | `core/model.py` | Невикористаний `import pygments`; `__repr__` звертається до неіснуючого `is_free` | `AttributeError` при repr і логуванні | F-01 |
| D12 | `adapters/base.py` | Анотація `parse → Course`, фактично повертається `list[Course]`; `source` задається всередині `parse` | Неузгоджений контракт адаптера | F-01 |
| D13 | `notify/digest.py` | Немає `html.escape` при `parse_mode=HTML`; немає поділу на частини до 4096 символів | Telegram відхиляє повідомлення, якщо в назві є `<` чи `&` або записів багато | E-07 |
| D14 | `adapters/epam.py` | `date_start` / `date_end` передаються рядками без розбору | Залежність від неявного перетворення драйвером | F-02 |
| D15 | репозиторій | `.gitignore` не містить `db-data/`; немає `.env.example`; БД Metabase створюється вручну | Ризик закомітити дані; розгортання невідтворюване | F-01, E-10 |
| D16 | `docker-compose.yml` | Пакети змонтовано двічі (`/opt/airflow/<pkg>` і `/opt/airflow/project/<pkg>`) | Залежно від шляху імпорту `BASE_DIR` вказує на різні теки, і `.env` може не знаходитися | F-01 |

---

## 4. Модель даних

### 4.1 Терміни

- **Джерело** (`source`) — `epam`, `softserve` тощо. Назва не містить `_`, бо джерело виділяється з ключа файлу.
- **Знімок** — один файл у MinIO, тобто повний список пропозицій джерела на момент `fetched_at`. Повноту перевіряє питання Q2.
- **Пропозиція** — пара `(source, source_id)`.
- **Версія** — рядок у `courses`. Активна версія має `active_to IS NULL`; у пропозиції не більше однієї активної версії.
- **Відстежувані поля** — поля, що входять у `content_hash` (§5.2).
- **Епізод** — безперервний проміжок присутності пропозиції в джерелі: від першої версії до закриття з `close_reason = 'removed'`. Якщо пропозиція знову з'являється, починається новий епізод.

### 4.2 Зміни схеми

Усі зміни виконуються лише через Alembic (`migrations/`). Перша ревізія — базова, відповідає поточним таблицям; для наявної БД її застосовують через `alembic stamp`. Та сама ревізія або наступна створює схему `analytics`.

**`file_record`**

- Додати `fetched_at TIMESTAMP NOT NULL`. Заповнення для наявних рядків: S3 `LastModified`, якщо недоступно — `uploaded_at`.
- Статуси: `pending`, `done`, `error`. Проміжний `processing` більше не комітиться; наявні рядки з `processing` міграція переводить у `pending`.

**`courses`**

- `content_hash VARCHAR(64) NULL`.
- `last_seen_at TIMESTAMP NULL`.
- `close_reason VARCHAR(16) NULL`, `CHECK (close_reason IN ('changed','removed'))`.
- `CHECK ((active_to IS NULL) = (close_reason IS NULL))`. Застосувати після replay або з очищенням старих рядків; рішення записати в журнал.
- `description TEXT NULL`, `tags JSONB NULL` — лише якщо I-01 підтвердить наявність таких даних у сирих файлах.
- `skills_version INT NULL` (E-01).
- Індекси:
  - `uq_active_course_per_source UNIQUE (source, source_id) WHERE active_to IS NULL`, оголошений через `__table_args__`;
  - `ix_courses_posting (source, source_id, active_from)`.

**`load_stats`** (E-08) — один рядок на оброблений файл:

| Стовпець | Тип | Примітка |
|---|---|---|
| `id` | serial PK | |
| `file_record_id` | int FK, UNIQUE | |
| `source` | varchar | |
| `snapshot_at` | timestamp | = `file_record.fetched_at` |
| `run_id` | varchar NULL | `run_id` Airflow або `replay` |
| `received` | int | кількість різних `source_id` у знімку, включно з відхиленими |
| `rejected` | int | 0 до E-05 |
| `duplicates` | int | повтори `source_id` у знімку |
| `inserted`, `changed`, `unchanged`, `closed` | int | |
| `closures_blocked` | bool | |
| `block_reason` | text NULL | |
| `parser_version` | int | |
| `duration_ms` | int | |
| `created_at` | timestamp | |

Інваріант: `inserted + changed + unchanged = received - rejected`.

**`skill`, `course_skill`** — див. §8.5. **`course_enrichment`** — див. §9.

### 4.3 Час

Усі мітки часу зберігаються в UTC без часового поясу (naive), як у поточній схемі. Єдине джерело часу — `utils/time.py: utcnow()`. Викликати `datetime.now()` чи `datetime.utcnow()` напряму не можна.

---

## 5. Завантаження знімків (`storage/loader.py`)

### 5.1 Порядок і транзакції

Функції завантажувача приймають `Session` і самі не комітять. Транзакціями керує той, хто їх викликає:

- у звичайному DAG — коміт після кожного файлу;
- у replay — `session.begin_nested()` (savepoint) на кожен файл усередині однієї зовнішньої транзакції.

`process_pending_files(session, run_id, commit_per_file=True) -> LoadReport`:

1. Вибрати `file_record` зі `status = 'pending'`, `ORDER BY fetched_at, id`.
2. Обробити кожен файл за алгоритмом §5.3.
3. При винятку: відкотити файл, окремо записати `status = 'error'` і `error_message` (до 1000 символів). Решту файлів цього джерела в поточному запуску пропустити (вони лишаються `pending`); файли інших джерел обробляти далі.
4. Якщо `fetched_at` менший за останній `snapshot_at` у `load_stats` для цього джерела, записати помилку `out-of-order snapshot`. Виправляється через replay.

`LoadReport` (серіалізується в dict для XCom):

```python
{
  "new_course_ids": [int],          # вставлені без активної версії (нові або повернуті)
  "per_source": {"epam": {"received": 0, "inserted": 0, "changed": 0,
                          "unchanged": 0, "closed": 0, "rejected": 0}},
  "blocked": [{"source": "softserve", "reason": "R1: received=0, active=37"}],
  "errors": [{"file_key": "raw/...", "message": "..."}],
}
```

### 5.2 `content_hash` (`core/hashing.py`)

Хеш — sha256 (hex) від канонічного JSON відстежуваних полів: `title, url, course_type, direction, format, level, price, date_start, date_end, status, country, city, languages`, а після I-01 також `description, tags`.

Нормалізація:

- рядки обрізаються, порожній рядок стає `null`;
- дати перетворюються через `isoformat()`;
- `languages` — відсортований список без повторів;
- `json.dumps(sort_keys=True, ensure_ascii=False, separators=(",", ":"))`.

У `adapters/base.py` є константа `PARSER_VERSION`. Будь-яка зміна набору полів, нормалізації чи логіки адаптера вимагає збільшити `PARSER_VERSION` і виконати replay. Інакше всі пропозиції виглядатимуть зміненими.

### 5.3 Алгоритм обробки одного знімка

`snapshot_at = file_record.fetched_at`

1. Прочитати об'єкт, виконати `json.loads` і `adapter.parse`, отримати кандидатів.
2. Прибрати дублікати за `source_id` (перемагає останній) і порахувати `duplicates`.
3. Завантажити активні версії джерела: `active = {source_id: Course}`.
4. Для кожного кандидата обчислити `h = content_hash` і діяти так:
   - **Немає активної версії** → вставити версію з `active_from = last_seen_at = snapshot_at` (`inserted`) і додати id в `new_course_ids`.
   - **Активна версія має `content_hash IS NULL`** (старий рядок до replay) → записати `h` і `last_seen_at`, нову версію не створювати (`unchanged`).
   - **Хеш збігається** → оновити `last_seen_at = snapshot_at` (`unchanged`).
   - **Хеш відрізняється** → у старій версії встановити `active_to = snapshot_at` і `close_reason = 'changed'`, **виконати `flush()` до вставки** (цього вимагає частковий унікальний індекс), потім вставити нову версію (`changed`).
5. `to_close = множина активних id − множина побачених id`. Відхилені записи з валідним `source_id` (E-05) теж вважаються побаченими.
6. Перевірити захист (§5.4). Якщо закриття дозволене, для кожної версії з `to_close` встановити `active_to = snapshot_at` і `close_reason = 'removed'` (`closed`).
7. Вставити рядок `load_stats`, встановити `file_record.status = 'done'` і `processed_at = utcnow()`.

### 5.4 Захист від масового закриття (E-06)

Параметри в `Settings` (значення за замовчуванням):

| Параметр | За замовч. |
|---|---|
| `guard_min_ratio` | 0.5 |
| `guard_history_n` | 7 |
| `guard_min_history` | 3 |
| `guard_max_close_share` | 0.5 |
| `guard_min_active` | 10 |

`baseline` — медіана `received` за останні `guard_history_n` рядків `load_stats` цього джерела з `closures_blocked = false`.

Закриття блокуються, якщо виконується хоча б одне правило:

- **R1:** `received == 0` і `len(active) > 0`.
- **R2:** є не менше `guard_min_history` попередніх знімків і `received < guard_min_ratio × baseline`.
- **R3:** `len(active) ≥ guard_min_active` і `len(to_close) / len(active) > guard_max_close_share`.

Якщо закриття заблоковано:

- вставки та зміни все одно застосовуються, пропускаються лише закриття;
- `load_stats.closures_blocked = true`, `block_reason` містить правило та числа (наприклад, `R2: received=3, baseline=41`);
- запис потрапляє в `LoadReport.blocked`, і надсилається сповіщення.

Наступний нормальний знімок закриє ці пропозиції. Дата закриття зсунеться не більш ніж на день; це задокументоване обмеження.

Якщо зламана структура файлу (немає очікуваного ключа верхнього рівня), це помилка файлу (§5.1), а не спрацювання захисту.

---

## 6. DAG

### 6.1 `load_courses_pipeline_dag`

Для всіх задач `default_args` задає `on_failure_callback = notify.alerts.task_failure_alert`.

```
get_sources
  → fetch_one_source.expand(source)
  → load_all_to_db        [trigger_rule=all_done]
  → extract_skills        [E-01]
  → notify_digest         [trigger_rule=all_done]
  → finalize              [trigger_rule=all_done]
```

- **`fetch_one_source`** піднімає виняток при будь-якій помилці: HTTP, невалідний JSON (перевіряти `json.loads` до збереження), MinIO, БД. Параметри `retries=3` і `retry_delay=2 хв` лишаються. Час знімка `fetched_at = utcnow()`; ключ `raw/{source}_{YYYYMMDDTHHMMSSZ}.json` у справжньому UTC.
- **`load_all_to_db`** повертає `LoadReport` через XCom.
- **`notify_digest`** формує повідомлення за §7.1.
- **`finalize`** завершується винятком без повторних спроб, якщо будь-яка попередня задача впала або `LoadReport.errors` не порожній. Так статус запуску DAG відповідає реальності.
- Імпорти проєктних модулів лишаються всередині функцій задач. Контейнер `airflow-dag-processor` не має залежностей проєкту.
- У фазі 2 після `extract_skills` додається задача `enrich_courses` (E-04).

Сигнатури колбеків, ключі контексту та виняток для негайного падіння задачі перевірити за документацією Airflow 3.1.

### 6.2 `replay_courses_dag` (E-03)

`schedule=None`, `catchup=False`. Параметри: `dry_run: bool = True`, `confirm: str = ""`. При `dry_run = false` параметр `confirm` має дорівнювати `"REPLAY"`, інакше задача падає.

1. **`sync_file_records`**: пройти всі об'єкти з префіксом `raw/` (з пагінацією). Для відсутніх у БД створити `file_record`: `source` береться з ключа, `fetched_at` = `LastModified` (UTC naive), також `etag` і `size`. Для наявних заповнити порожній `fetched_at`.
2. **`backup`** (лише якщо не `dry_run`): `CREATE TABLE backup_<YYYYMMDDHHMM>_<table> AS SELECT *` для `courses`, `load_stats`, `course_skill`, `course_enrichment` (тих, що існують).
3. **`rebuild`** в одній зовнішній транзакції:
   - `TRUNCATE` похідних таблиць з `RESTART IDENTITY`;
   - `UPDATE file_record SET status='pending', error_message=NULL, processed_at=NULL`;
   - `process_pending_files(commit_per_file=False)` із savepoint на кожен файл;
   - витягування навичок і збагачення, якщо вони вже реалізовані;
   - при `dry_run` зібрати звіт і виконати `ROLLBACK`, інакше `COMMIT`.
4. **`report`**: один підсумок (кількість файлів, помилки, пропозиції, версії, заблоковані знімки, тривалість). При `dry_run` звіт лише логується, інакше надсилається в Telegram. Окремі сповіщення захисту під час replay не надсилаються.

Replay ніколи не видаляє й не змінює об'єкти в MinIO.

---

## 7. Сповіщення

### 7.1 Дайджест (`notify/digest.py`)

- Дані беруться з `LoadReport` у XCom, а не за датою `created_at`.
- Заголовок: `🆕 Нові пропозиції: N`. Рядки у поточному форматі (`🆓`/`💰`, назва жирним, джерело), плюс посилання, якщо є `url`.
- Підсумковий рядок: `Змінено: X · Закрито: Y`.
- Для заблокованих джерел: `⚠️ Закриття пропущено: softserve (R1…)`. Для помилок: `❗ Помилки обробки: K файлів`.
- Якщо нових немає, лишається поточний текст `Сьогодні нових курсів немає.` і підсумковий рядок, якщо в ньому є ненульові значення.
- Усі підставлені значення проходять `html.escape`. Повідомлення ділиться за рядками на частини до 4000 символів.

### 7.2 Сповіщення про збої (`notify/alerts.py`, E-07)

- `task_failure_alert(context)` надсилає `❌ Збій задачі` з `dag_id`, `task_id`, `run_id`, `try_number` і першими 300 символами винятку (екранованими). Посилання на лог додається, якщо воно доступне в контексті.
- `send_alert(text)` надсилає в `TELEGRAM_ALERT_CHAT_ID`, а якщо його не задано — у `TELEGRAM_CHAT_ID`.
- Колбек ніколи не піднімає виняток: помилки перехоплюються й логуються.

### 7.3 Режим без надсилання

Якщо `NOTIFY_DRY_RUN=true`, `send_telegram_message` логує текст на рівні INFO замість надсилання. У `.env.example` значення за замовчуванням — `true`.

---

## 8. Витягування навичок (E-01)

### 8.1 Дослідження вхідних даних (I-01)

Скрипт `scripts/inspect_raw.py` для останнього файлу кожного джерела виводить дерево ключів JSON (типи, довжини списків), приклади текстових полів довше 50 символів, наявність полів пагінації або загальної кількості. Висновки записуються в §15.

### 8.2 Текст для аналізу

Використовуються поля `title`, `direction`, `description`, `tags` — ті, що існують. Обробка: прибрати HTML-теги, виконати `html.unescape`, стиснути пробіли.

### 8.3 Таксономія `enrich/skills_taxonomy.yaml`

```yaml
skills:
  - name: JavaScript
    category: language
    aliases: [javascript, js, ecmascript]
  - name: .NET
    category: backend
    aliases: [.net, dotnet, asp.net]
  - name: Go
    category: language
    aliases: [golang]
    strict_aliases: [Go]      # лише точний регістр, лише в title/tags
direction_map:                 # значення direction з EPAM → навички
  CloudAndDevOps: [AWS, Azure, Docker, Kubernetes]
```

Категорії: `language, frontend, backend, mobile, database, data, ml, cloud, devops, qa, security, analysis_pm, design`. Обсяг — 100–150 навичок.

### 8.4 Зіставлення (`enrich/skills.py`)

- Чиста функція без доступу до БД: `extract(fields: dict[str, str | None]) -> set[Match(skill, matched_text, field)]`.
- `aliases` шукаються без урахування регістру з межами `(?<![A-Za-z0-9+#.])` + alias + `(?![A-Za-z0-9+#])`. Такі межі коректно обробляють `C#`, `C++`, `.NET`, `Node.js` і не знаходять `Java` всередині `JavaScript`.
- `strict_aliases` (Go, R, C тощо) шукаються з урахуванням регістру й лише в `title` і `tags`.
- Константа `SKILLS_VERSION` збільшується при будь-якій зміні таксономії чи алгоритму.

### 8.5 Зберігання та запуск

- `skill(id serial PK, name UNIQUE, category)`.
- `course_skill(course_id FK → courses ON DELETE CASCADE, skill_id FK → skill, matched_text, field, PRIMARY KEY (course_id, skill_id))`.
- Задача `extract_skills`:
  1. синхронізує таксономію з таблицею `skill` (upsert за `name`);
  2. обробляє версії з `skills_version IS NULL OR skills_version < SKILLS_VERSION`: видаляє їхні `course_skill`, вставляє нові, записує `skills_version`.
- Навички прив'язані до версії, бо текст належить версії.

### 8.6 Ручна перевірка

Скрипт `scripts/skills_sample.py` виводить 20 випадкових активних пропозицій зі знайденими навичками й фрагментами збігів для ручного перегляду.

---

## 9. Класифікація (E-04, фаза 2)

- Таблиця `course_enrichment(course_id PK FK → courses ON DELETE CASCADE, level_norm, format_norm, city_norm, country_norm, rules_version INT)`.
- Допустимі значення: `level_norm ∈ {intern, junior, middle, senior, unknown}`, `format_norm ∈ {online, offline, hybrid, unknown}`.
- Правила в `enrich/classify.py`, словник міст і країн в `enrich/geo_aliases.yaml` (наприклад, Київ / Kyiv / Kiev → Kyiv). Вхідні поля: `level`, `course_type`, `title` (ключові слова), `format`, `city`, `country`.
- Якщо місто треба додатково витягувати в адаптері (наприклад, EPAM `PlanLocations`), це зміна адаптера: збільшити `PARSER_VERSION` і виконати replay.
- Похідні поля не входять у `content_hash`. Підхід до версіонування такий самий, як для навичок (`RULES_VERSION`).

---

## 10. Якість даних і карантин (E-05, фаза 2)

- `core/validation.py`: Pydantic-модель `CourseIn` перевіряє кожен запис після мапінгу:
  - `source_id` не порожній;
  - `title` не порожній і не довший за 500 символів;
  - `url` починається з `http(s)` або `null`;
  - `date_end ≥ date_start`, якщо обидві дати задані;
  - `languages` — `list[str]` або `null`.
- Адаптер повертає `ParseResult(valid: list[Course], rejected: list[Rejected(source_id | None, reason, raw_item)])`.
- Відхилені записи зберігаються в MinIO як `quarantine/{source}/{file_stem}.jsonl` (один об'єкт на знімок, рядки `{source_id, reason, item}`); їхня кількість — у `load_stats.rejected`.
- Відхилені записи з `source_id` вважаються побаченими й не закриваються (§5.3, п. 5).

---

## 11. Аналітичні представлення (`sql/views/`, схема `analytics`)

Файли мають вигляд `sql/views/NN_name.sql`, кожен містить `DROP VIEW IF EXISTS ... CASCADE; CREATE VIEW ...`. Застосовуються командою `python -m scripts.apply_views` у порядку номерів; команда ідемпотентна. Представлення не входять в Alembic.

| Представлення | Один рядок = | Основні стовпці | Пункт |
|---|---|---|---|
| `v_active_courses` | активна версія | поля курсу, `skills text[]` (+ збагачення) | E-02, E-01 |
| `v_posting_episodes` | епізод | `source, source_id, title` (остання версія), `first_seen, removed_at, is_active, lifetime_days, versions_count, left_censored` | E-02 |
| `v_daily_activity` | день × джерело | `day, source, new, changed, removed, active_end_of_day` | E-02 |
| `v_field_changes` | змінене поле | `source, source_id, changed_at, field_name` | E-02 |
| `v_skill_counts` | навичка | `skill, category, active_postings, all_time_postings` | E-01 |
| `v_skill_weekly` | тиждень × навичка | `week, skill, postings_active` | E-01 |
| `v_skill_pairs` | пара навичок | `skill_a, skill_b, postings` (≥ 2, за останньою версією пропозиції) | E-01 |
| `v_pipeline_health` | знімок | поля `load_stats` + `file_status`, `error_message` | E-08 |

Семантика:

- `lifetime_days` = `(coalesce(removed_at, останній snapshot_at джерела) − first_seen)` у днях з одним знаком після коми. Використовується останній знімок, а не `now()`, щоб зупинка пайплайна не завищувала тривалість.
- `left_censored = true`, якщо `first_seen` збігається з першим знімком джерела: справжній початок невідомий.
- Медіана тривалості (`percentile_cont(0.5)`) рахується лише за епізодами з `removed_at IS NOT NULL AND NOT left_censored`.
- У `v_daily_activity`: `new` — епізоди, що почалися цього дня; `removed` — ті, що завершилися; `changed` — версії, закриті з `close_reason = 'changed'`; `active_end_of_day` будується через `generate_series` від першого знімка до останнього.
- `v_field_changes`: сусідні версії пропозиції (за `active_from`), де попередня має `close_reason = 'changed'`. Відстежувані поля порівнюються через `IS DISTINCT FROM`, кожне змінене поле дає окремий рядок (`LATERAL (VALUES ...)`).

---

## 12. Metabase

### 12.1 Дашборди (E-09)

Питання в Metabase — це нативні SQL-запити до представлень `analytics` без складної логіки. Текст кожного питання зберігається в `metabase/cards/NN_name.sql`.

**Дашборд «Ринок ІТ-стажувань»** (фільтри: джерело, період)

| # | Питання | Візуалізація | Джерело |
|---|---|---|---|
| 1 | Активні пропозиції зараз | число | `v_active_courses` |
| 2 | Нові пропозиції за днями | стовпчики за джерелами | `v_daily_activity` |
| 3 | Активні пропозиції в динаміці | лінія | `v_daily_activity` |
| 4 | Топ-15 технологій серед активних | горизонтальні стовпчики | `v_skill_counts` |
| 5 | Тренд топ-5 технологій за тижнями | лінія | `v_skill_weekly` |
| 6 | Технології, що зустрічаються разом | таблиця | `v_skill_pairs` |
| 7 | Медіанна тривалість життя за джерелом і типом | таблиця / стовпчики | `v_posting_episodes` |
| 8 | Розподіл тривалості життя (кошики по 7 днів) | гістограма | `v_posting_episodes` |
| 9 | Поля, що змінюються найчастіше | стовпчики | `v_field_changes` |

**Дашборд «Здоров'я пайплайна»**

| # | Питання | Візуалізація | Джерело |
|---|---|---|---|
| 10 | Отримано записів за знімками по джерелах | лінія | `v_pipeline_health` |
| 11 | Заблоковані закриття й помилки файлів | таблиця | `v_pipeline_health` |
| 12 | Частка відхилених записів (після E-05) | лінія | `v_pipeline_health` |

### 12.2 Відтворюваність (E-10, фаза 2)

- **Створення БД Metabase:** скрипт `docker/postgres-init/01-metabase-db.sh` створює базу `$MB_DB_DBNAME` і монтується в `/docker-entrypoint-initdb.d`. Потрібен саме `.sh`, бо у `.sql` змінні не підставляються. Скрипт спрацьовує лише на порожній теці даних; для наявної БД ручний крок описується в README.
- **Роль лише для читання `metabase_ro`:** `CONNECT` і `USAGE` на схему `analytics`, `SELECT` на її представлення. Пароль береться з `.env`. Metabase підключається до даних через цю роль.
- **Скрипт `scripts/metabase_provision.py`** (ідемпотентний):
  1. якщо Metabase ще не налаштовано, завершує налаштування через setup-токен;
  2. входить під адміністратором;
  3. створює або оновлює підключення «WorkFinder»;
  4. створює колекцію;
  5. створює або оновлює питання за назвою з `metabase/cards/*.sql`;
  6. збирає дашборди за описом у `metabase/dashboards.yaml`.
- Формати ендпоінтів перевірити за документацією API Metabase **для закріпленої версії v0.53.4** до початку реалізації.

---

## 13. Конфігурація (`.env`)

| Змінна | Статус | Призначення |
|---|---|---|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | є | БД даних |
| `POSTGRES_URL` | є (формується в compose) | SQLAlchemy URL |
| `PGADMIN_EMAIL`, `PGADMIN_PASSWORD` | є | pgAdmin |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_BUCKET_NAME`, `MINIO_ENDPOINT` | є | MinIO |
| `CAREERS_URL_<SOURCE>` | є | URL джерел |
| `TELEGRAM_TOKEN`, `TELEGRAM_CHAT_ID` | є | дайджест |
| `AIRFLOW_SECRET_KEY`, `AIRFLOW_JWT_SECRET` | є | Airflow |
| `MB_DB_DBNAME` | є | внутрішня БД Metabase |
| `TELEGRAM_ALERT_CHAT_ID` | нова, опційна | чат для збоїв |
| `NOTIFY_DRY_RUN` | нова | `true` — лише логувати |
| `GUARD_MIN_RATIO`, `GUARD_HISTORY_N`, `GUARD_MIN_HISTORY`, `GUARD_MAX_CLOSE_SHARE`, `GUARD_MIN_ACTIVE` | нові | §5.4 |
| `METABASE_ADMIN_EMAIL`, `METABASE_ADMIN_PASSWORD`, `METABASE_RO_PASSWORD` | нові (E-10) | §12.2 |

Кожна нова змінна додається одночасно в `Settings` (зі значенням за замовчуванням, якщо змінна опційна) і в `.env.example`.

---

## 14. Критерії приймання

SQL виконується в сервісі `postgres` (команду наведено в `CLAUDE.md`).

**F-01 — Гігієна репозиторію та схема**

- `docker compose run --rm migrate` успішно виконується і на наявній, і на порожній БД.
- Індекс існує:
  ```sql
  SELECT indexdef FROM pg_indexes
  WHERE tablename = 'courses' AND indexname = 'uq_active_course_per_source';
  ```
  Результат — 1 рядок з `WHERE (active_to IS NULL)`.
- Імпорт `notify.digest`, `notify.telegram` та інших модулів не має побічних ефектів; `notify/courses.py` відсутній.
- `airflow dags list-import-errors` не показує помилок.

**I-01 — Дослідження сирих даних**

- Питання Q1–Q5 у §15 мають записані відповіді.

**F-02 — Коректне версіонування**

- Два запуски DAG поспіль без змін у джерелі: у другому `inserted = 0`, `changed = 0`, `unchanged = received`.
- Немає дублікатів активних версій:
  ```sql
  SELECT source, source_id FROM courses WHERE active_to IS NULL
  GROUP BY 1, 2 HAVING count(*) > 1;
  ```
  Результат — 0 рядків.
- Статус закриття узгоджений:
  ```sql
  SELECT count(*) FROM courses WHERE (active_to IS NULL) <> (close_reason IS NULL);
  ```
  Результат — 0.

**F-03 — Прозорі збої**

- Із тимчасовим `CAREERS_URL_BROKEN=https://example.invalid`:
  - `fetch_one_source[broken]` падає після повторних спроб;
  - інші джерела завантажуються;
  - `finalize` падає, запуск DAG має статус failed;
  - у логах є текст сповіщення (dry-run).
- Жоден файл не завис:
  ```sql
  SELECT count(*) FROM file_record WHERE status = 'processing';
  ```
  Результат — 0.

**E-08 — Метрики завантажень**

- Кожен оброблений файл має рядок статистики:
  ```sql
  SELECT count(*) FROM file_record f
  LEFT JOIN load_stats s ON s.file_record_id = f.id
  WHERE f.status = 'done' AND s.id IS NULL;
  ```
  Результат — 0.
- Лічильники узгоджені:
  ```sql
  SELECT count(*) FROM load_stats
  WHERE inserted + changed + unchanged <> received - rejected;
  ```
  Результат — 0.

**E-06 — Закриття зниклих і захист**

- `python -m scripts.simulate_guard --source softserve --empty` обробляє синтетичний знімок у транзакції з відкатом і виводить `blocked: R1`.
- `--keep-share 0.2` виводить `blocked: R3`.
- Після скрипта БД не змінилася (кількість рядків до й після однакова).

**E-07 — Сповіщення**

- У режимі dry-run у логах є текст дайджесту та сповіщення про збій.
- Назва з символами `<`, `&`, `>` екранується.
- Дайджест довший за 4000 символів ділиться на частини.

**E-03 — Replay**

- Запуск із `dry_run=true` лише логує звіт; кількість рядків у `courses` і `load_stats` не змінюється.
- Запуск із `dry_run=false, confirm=REPLAY` створює таблиці `backup_*`.
- Кількість `file_record` зі статусом `done` дорівнює кількості об'єктів у `raw/` мінус файли з помилками, перелічені у звіті.
- Середня кількість версій на пропозицію значно менша за кількість знімків:
  ```sql
  SELECT round(avg(c), 2)
  FROM (SELECT count(*) c FROM courses GROUP BY source, source_id) t;
  ```

**E-02 — Тривалість життя**

- Усі представлення з §11 для E-02 існують і повертають дані.
- Немає від'ємної тривалості:
  ```sql
  SELECT count(*) FROM analytics.v_posting_episodes WHERE lifetime_days < 0;
  ```
  Результат — 0.

**E-01 — Навички**

- Покриття активних пропозицій (фактичне значення записати в журнал рішень):
  ```sql
  SELECT round(100.0 * count(*) FILTER (WHERE has) / count(*), 1)
  FROM (
    SELECT EXISTS (SELECT 1 FROM course_skill cs WHERE cs.course_id = c.id) AS has
    FROM courses c WHERE c.active_to IS NULL
  ) t;
  ```
- Ручна перевірка `scripts/skills_sample.py`: немає хибних збігів на кшталт Java у JavaScript, C у C#, Go у звичайному слові.

**E-09 — Дашборди**

- Обидва дашборди відкриваються без помилок, фільтри працюють.
- Кожне питання має SQL-файл у `metabase/cards/`.

**E-04 — Класифікація**

- Кожна активна версія має рядок у `course_enrichment`.
- Частка `unknown` за кожним полем зафіксована в журналі рішень.

**E-05 — Якість і карантин**

- Синтетичний запис без `title` потрапляє в карантин, `rejected ≥ 1`, пропозиція не закривається.

**E-10 — Відтворюваність Metabase**

- На чистій машині послідовність `docker compose up -d` → `migrate` → `apply_views` → запуск DAG → `metabase_provision` дає обидва дашборди без ручних дій.
- Повторний запуск `metabase_provision` не створює дублікатів.

---

## 15. Відкриті питання

Відповіді записує Claude Code під час I-01. Рішення, що змінюють специфікацію, також фіксуються в журналі рішень у `ROADMAP.md`.

| ID | Питання | Впливає на | Відповідь / рішення |
|---|---|---|---|
| Q1 | Які текстові поля (опис, теги, навички) є в сирому JSON кожного джерела? | E-01, F-02 (поля хешу) | — |
| Q2 | Чи містить кожен файл повний список пропозицій джерела, без пагінації? Порівняти кількість елементів із полем загальної кількості, якщо воно є. | E-06 | — |
| Q3 | Чи стабільний URL EPAM? Якщо в ньому є buildId Next.js (`_next/data/<id>/...`), він змінюється після кожного деплою сайту. | F-03, E-07 | — |
| Q4 | Скільки файлів у `raw/` і за який період? Чи є пропуски днів? | E-03, E-02 | — |
| Q5 | Які значення має `status` у кожному джерелі й що вони означають (реєстрацію відкрито чи закрито)? | E-02 (можлива друга метрика: тривалість відкритої реєстрації) | — |
| Q6 | Фільтр EPAM (Україна, `PlanLevel > 2`): якщо пропозиція перестає під нього підпадати, вона вважається закритою. Чи прийнятно це? | E-06, E-02 | — |

# CLAUDE.md

WorkFinder — ETL-пайплайн на Apache Airflow 3.1. Щодня збирає пропозиції ІТ-курсів і стажувань (EPAM, SoftServe), зберігає сирі JSON у MinIO, веде версіоновану історію в PostgreSQL, надсилає дайджест у Telegram і показує аналітику в Metabase.

## Документи

- `SPEC.md` — що і як будувати. Це джерело правди.
- `ROADMAP.md` — порядок робіт, статуси, журнал рішень.

Порядок роботи над задачею:

1. Знайти пункт у `ROADMAP.md` і прочитати всі розділи SPEC, на які він посилається.
2. Виконувати один пункт за раз, дотримуючись порядку з ROADMAP.
3. Після завершення перевірити критерії з SPEC §14, позначити пункт `[x]` і додати рядок у журнал рішень.
4. Якщо SPEC суперечить коду або реальним даним, зупинитися, описати розбіжність і запропонувати правку SPEC. Не вигадувати поведінку мовчки.

## Структура

```
adapters/    парсери джерел (BaseAdapter → list[Course])
core/        db, model, repository, setting (pydantic-settings), hashing
storage/     minio (boto3), loader (обробка знімків)
notify/      telegram, digest, alerts
enrich/      навички, класифікація (з'являється в E-01 / E-04)
dags/        load_courses_dag.py, replay_courses_dag.py
migrations/  Alembic
sql/views/   аналітичні представлення (схема analytics)
metabase/    SQL питань і опис дашбордів
scripts/     допоміжні скрипти (python -m scripts.<name>)
```

## Команди

```bash
docker compose up -d --build                      # весь стек
docker compose run --rm migrate                    # alembic upgrade head (після F-01)

# Airflow (задачі виконуються в airflow-scheduler, LocalExecutor)
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags test load_courses_pipeline_dag
docker compose exec airflow-scheduler airflow dags trigger replay_courses_dag \
  --conf '{"dry_run": true}'

# Скрипти проєкту
docker compose exec -w /opt/airflow/project airflow-scheduler python -m scripts.inspect_raw
docker compose exec -w /opt/airflow/project airflow-scheduler python -m scripts.apply_views

# SQL
docker compose exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Інтерфейси: Airflow `:8081`, pgAdmin `:8080`, MinIO `:9001`, Metabase `:3000`.

## Правила роботи з даними (обов'язкові)

- Об'єкти MinIO з префіксом `raw/` — джерело правди. Їх можна лише читати й додавати нові; ніколи не видаляти й не перезаписувати.
- Не викликати `reset_db()`, `drop_all`, `DROP TABLE` чи `TRUNCATE` поза `replay_courses_dag`. Replay спершу запускається з `dry_run=true`.
- Схема змінюється лише через нову ревізію Alembic. Представлення змінюються лише файлами в `sql/views/`.
- Під час розробки має бути `NOTIFY_DRY_RUN=true`. Реальні повідомлення в Telegram — лише на явний запит.
- Не читати, не виводити й не комітити `.env`. Нова змінна додається одночасно в `Settings` і `.env.example`.
- Зміна полів хешу, нормалізації чи логіки адаптера означає: збільшити `PARSER_VERSION` і попередити, що потрібен replay.

## Код

- Python 3.12. Версії залежностей обмежено constraints Airflow 3.1 (див. `Dockerfile`); нові пакети додавати з тими самими constraints.
- Airflow 3: імпорти з `airflow.sdk` (`dag`, `task`), TaskFlow API. Якщо не впевнений у сигнатурі чи поведінці, перевірити документацію Airflow 3.1, а не покладатися на API Airflow 2.
- У файлах DAG на верхньому рівні лише легкі імпорти. Проєктні модулі імпортуються всередині функцій задач, бо `airflow-dag-processor` не має залежностей проєкту.
- Функції завантажувача та збагачення отримують `Session` і не комітять самі (SPEC §5.1).
- Для нового коду SQLAlchemy — `select()`-стиль.
- Час — лише через `utils.time.utcnow()`: UTC без часового поясу.
- Логування — через `utils.logger.get_logger(__name__)`, без `print` (крім скриптів у `scripts/`).
- Анотації типів для публічних функцій. Невеликі функції з однією відповідальністю.
- Код, коментарі й повідомлення комітів — англійською, у стилі Conventional Commits, як у наявній історії (`feat(db): ...`, `fix(loader): ...`). Тексти в Telegram, `SPEC.md` і `ROADMAP.md` — українською.
- Один пункт ROADMAP = один або кілька логічних комітів. Комітити лише після перевірки.

## Перевірка

Автоматизовані тести та CI зараз поза обсягом. Перевірка виконується на реальному стеку:

1. `migrate` проходить без помилок, `list-import-errors` порожній.
2. `airflow dags test ...` завершується очікуваним статусом.
3. SQL-перевірки з SPEC §14 для поточного пункту дають очікувані результати.
4. Коротко звітувати, що саме перевірено і з якими числами.

## Поза обсягом

Не додавати без запиту: рекомендації (TF-IDF, ембеддинги, pgvector), інтерактивного бота, REST API, dbt, CI, Makefile, тестову інфраструктуру, нові джерела (до завершення фази 1).

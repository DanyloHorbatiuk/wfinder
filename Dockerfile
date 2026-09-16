FROM apache/airflow:3.1.0

USER airflow
RUN pip install --no-cache-dir \
    sqlalchemy \
    boto3 \
    psycopg2-binary \
    python-dotenv \
    curl-cffi \
    pydantic_settings \
    httpx \
    alembic \
    pyyaml \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt"
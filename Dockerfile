FROM apache/airflow:2.11.0

RUN python -m venv dbt_env &&  \
    source dbt_env/bin/activate && \
    pip install --upgrade pip &&  \
    pip install --no-cache-dir dbt-core==1.10.9 dbt-athena==1.9.4 &&  \
    deactivate

COPY pyproject.toml uv.lock /opt/airflow/

RUN uv pip compile pyproject.toml > requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

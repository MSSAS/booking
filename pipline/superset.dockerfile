FROM apache/superset:3.1.3

USER root
RUN pip install --no-cache-dir clickhouse-connect
USER superset

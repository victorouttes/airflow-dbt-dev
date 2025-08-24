# airflow-dbt-dev
Projeto base para utilização do airflow com:
* Extração de dados de APIs para a camada landing
* Carga de dados na camada bronze em Iceberg com SCD tipo 2
* Transformação dos dados nas demais camadas com DBT

### Como rodar localmente
```bash
docker compose up -d
```

Entrar em `http://localhost:8080/` com as credenciais:
```
login: airflow
senha: airflow
```

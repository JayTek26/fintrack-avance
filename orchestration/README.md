# Orchestration

Ce dossier contient un exemple de DAG Airflow qui orchestre le pipeline complet :

```
ingestion (Python) → COPY INTO Snowflake → dbt build → tests → reverse ETL
```

Voir `airflow_dag_fintrack.py` pour l'exemple minimal.

## Alternatives

- **dbt Cloud jobs** avec triggers webhook — plus simple, moins flexible
- **Dagster** — modélisation par assets, meilleure lisibilité
- **Prefect** — flows dynamiques

Le sprint 5 (voir MISSION.md) porte sur cette partie.

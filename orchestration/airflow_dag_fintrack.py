"""
FinTrack — DAG Airflow de production : ingestion → dbt → reverse ETL.

Cible : Airflow 2.7+ (paramètre `sla` des tâches, supprimé en Airflow 3).
Providers : apache-airflow-providers-snowflake, -common-sql, -slack, -http.

Fraîcheur contractuelle : 2h (gold), 6h (silver), 24h (bronze). Le DAG tourne
toutes les heures : le tier le plus exigeant (gold, 2h) est tenu avec une
marge d'un run raté. Un run incrémental complet prend ~2 min en dev (XSMALL,
10M transactions) — cf. docs/PERF_LOG.md.

Connexions Airflow attendues :
    snowflake_ingestion   rôle FINTRACK_INGESTION_ROLE, warehouse WH_INGESTION
    slack_data_alerts     webhook Slack (#data-platform)
    hightouch_api         https://api.hightouch.io + token dans le mot de passe

Variables Airflow :
    fintrack_landing_dir  répertoire de dépôt des fichiers du core banking
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

DBT_DIR = "/opt/dbt/fintrack_perf_scale"
DBT = f"cd {DBT_DIR} && dbt"
SNOWFLAKE_CONN = "snowflake_ingestion"
SLACK_CONN = "slack_data_alerts"
HIGHTOUCH_CONN = "hightouch_api"
HIGHTOUCH_SYNC_ID = "fintrack_sync"


# =============================================
# Notifications Slack
# =============================================
def _slack(text: str) -> None:
    # Une alerte qui échoue ne doit pas masquer l'erreur d'origine
    try:
        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN).send(text=text)
    except Exception as exc:  # noqa: BLE001
        print(f"Échec de l'envoi Slack : {exc}")


def notify_failure(context: dict) -> None:
    ti = context["task_instance"]
    _slack(
        ":red_circle: *FinTrack — échec* "
        f"`{ti.dag_id}.{ti.task_id}` (essai {ti.try_number})\n"
        f"Run : {context.get('run_id')}\n"
        f"Logs : {ti.log_url}"
    )


def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    _slack(
        ":warning: *FinTrack — SLA dépassé* sur "
        f"`{dag.dag_id}` : {task_list}\n"
        f"Tâches bloquantes : {blocking_task_list}"
    )


# =============================================
# Retries différenciés
# =============================================
# Ingestion : erreurs réseau / fichiers en retard → retries rapprochés,
# backoff exponentiel.
RETRY_INGESTION = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}
# dbt : les MERGE sont idempotents, mais une erreur dbt est rarement
# transitoire → un seul retry, espacé (laisse le warehouse reprendre).
RETRY_DBT = {"retries": 1, "retry_delay": timedelta(minutes=5)}
# Reverse ETL : API externe, erreurs 429/5xx fréquentes → retries nombreux.
RETRY_REVERSE_ETL = {
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}

default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,  # remplacé par Slack
    "email_on_retry": False,
    "on_failure_callback": notify_failure,
    **RETRY_DBT,
}


with DAG(
    dag_id="fintrack_hourly_pipeline",
    default_args=default_args,
    description="FinTrack — ingestion + dbt incrémental + reverse ETL (SLA 2h gold)",
    schedule="15 * * * *",  # toutes les heures à :15
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # deux MERGE concurrents sur la même table = conflits
    # < 2h : au-delà, la fraîcheur gold est de toute façon perdue ; aligné
    # sur le dernier SLA de tâche (wait_hightouch_sync, 115 min)
    dagrun_timeout=timedelta(minutes=115),
    sla_miss_callback=notify_sla_miss,
    tags=["fintrack", "dbt", "prod"],
) as dag:

    # =============================================
    # Ingestion : fichiers → stage → COPY INTO RAW
    # =============================================
    with TaskGroup("ingestion") as ingestion:

        wait_for_files = FileSensor(
            task_id="wait_for_files",
            filepath="{{ var.value.fintrack_landing_dir }}/raw_transactions*.csv.gz",
            poke_interval=60,
            timeout=30 * 60,
            mode="reschedule",  # ne bloque pas un worker pendant l'attente
            soft_fail=True,  # pas de fichier cette heure-ci : on saute, sans alerte
            sla=timedelta(minutes=35),
            **RETRY_INGESTION,
        )

        put_to_stage = BashOperator(
            task_id="put_to_stage",
            bash_command=(
                "snowsql -c fintrack_ingestion -o exit_on_error=true -q "
                "\"PUT file://{{ var.value.fintrack_landing_dir }}/*.csv.gz "
                "@FINTRACK_PROD.RAW.FINTRACK_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE\""
            ),
            sla=timedelta(minutes=45),
            **RETRY_INGESTION,
        )

        copy_into_raw = SQLExecuteQueryOperator(
            task_id="copy_into_raw",
            conn_id=SNOWFLAKE_CONN,
            sql="sql/copy_into_raw.sql",  # COPY INTO du script 03
            split_statements=True,
            sla=timedelta(minutes=50),
            **RETRY_INGESTION,
        )

        archive_files = BashOperator(
            task_id="archive_files",
            bash_command=(
                "mkdir -p {{ var.value.fintrack_landing_dir }}/archive/{{ ds }} && "
                "mv {{ var.value.fintrack_landing_dir }}/*.csv.gz "
                "{{ var.value.fintrack_landing_dir }}/archive/{{ ds }}/"
            ),
            **RETRY_INGESTION,
        )

        wait_for_files >> put_to_stage >> copy_into_raw >> archive_files

    # Les transformations tournent même sans nouveau fichier (soft_fail du
    # sensor → skipped) : les mises à jour de statut arrivent aussi par
    # d'autres flux. En revanche un échec d'ingestion bloque dbt.
    ingestion_done = EmptyOperator(
        task_id="ingestion_done",
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    # =============================================
    # dbt : fraîcheur → snapshot → staging → critique → marts
    # =============================================
    # `dbt build` (et non run + test) : les tests d'un modèle bloquent son
    # aval dans le même run, et les tests unitaires s'exécutent.
    with TaskGroup("dbt") as dbt_group:

        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=f"{DBT} deps",
        )

        # Échec si une source dépasse son error_after (6h sur raw_transactions)
        source_freshness = BashOperator(
            task_id="source_freshness",
            bash_command=f"{DBT} source freshness --target prod",
            sla=timedelta(minutes=55),
        )

        dbt_snapshot = BashOperator(
            task_id="dbt_snapshot",
            bash_command=f"{DBT} snapshot --target prod",
            sla=timedelta(minutes=60),
        )

        build_staging = BashOperator(
            task_id="build_staging",
            bash_command=f"{DBT} build --target prod --select tag:staging",
            sla=timedelta(minutes=70),
        )

        # Les faits critiques (fct_transactions) d'abord : c'est ce que
        # consomment les dashboards soumis au SLA gold
        build_critical = BashOperator(
            task_id="build_critical",
            bash_command=(
                f"{DBT} build --target prod --select tag:critical "
                "--exclude tag:staging tag:experimental"
            ),
            sla=timedelta(minutes=80),
        )

        build_marts = BashOperator(
            task_id="build_marts",
            bash_command=(
                f"{DBT} build --target prod --select tag:marts "
                "--exclude tag:critical tag:experimental"
            ),
            sla=timedelta(minutes=95),
        )

        (
            dbt_deps
            >> source_freshness
            >> dbt_snapshot
            >> build_staging
            >> build_critical
            >> build_marts
        )

    # =============================================
    # Reverse ETL : Hightouch → CRM / outils tenants
    # =============================================
    with TaskGroup("reverse_etl") as reverse_etl:

        trigger_sync = HttpOperator(
            task_id="trigger_hightouch_sync",
            http_conn_id=HIGHTOUCH_CONN,
            endpoint=f"/api/v1/syncs/{HIGHTOUCH_SYNC_ID}/trigger",
            method="POST",
            headers={"Content-Type": "application/json"},
            data="{}",
            response_check=lambda r: r.status_code in (200, 201, 202),
            sla=timedelta(minutes=100),
            **RETRY_REVERSE_ETL,
        )

        wait_sync = HttpSensor(
            task_id="wait_hightouch_sync",
            http_conn_id=HIGHTOUCH_CONN,
            endpoint=f"/api/v1/syncs/{HIGHTOUCH_SYNC_ID}",
            response_check=lambda r: r.json().get("status") in ("success", "warning"),
            poke_interval=60,
            timeout=20 * 60,
            mode="reschedule",
            sla=timedelta(minutes=115),
            **RETRY_REVERSE_ETL,
        )

        trigger_sync >> wait_sync

    ingestion >> ingestion_done >> dbt_group >> reverse_etl


# ============================================================
# Hebdomadaire : tests complets (hors fenêtre incrémentale)
# ============================================================
# Le run horaire restreint les tests des grosses tables à la fenêtre
# incrémentale (macro get_where_subquery). Une fois par semaine, on teste
# tout l'historique.
with DAG(
    dag_id="fintrack_weekly_full_tests",
    default_args=default_args,
    description="FinTrack — tests dbt complets sur tout l'historique",
    schedule="0 5 * * 0",  # dimanche 05:00
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fintrack", "dbt", "prod", "quality"],
) as weekly_dag:

    BashOperator(
        task_id="dbt_test_full",
        bash_command=(
            f"{DBT} deps && {DBT} test --target prod "
            "--exclude tag:experimental --vars '{tests_complets: true}'"
        ),
        execution_timeout=timedelta(hours=2),
        sla=timedelta(hours=2),
    )

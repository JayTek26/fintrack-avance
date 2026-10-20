# FinTrack Perf & Scale

Projet avancé de la série FinTrack Analytics — **niveau senior**.

Voir [`MISSION.md`](./MISSION.md) pour le briefing métier complet et les user stories sprintées.

---

## Contenu du repo

```
fintrack-perf-scale/
│
├── MISSION.md                         Briefing métier + user stories par sprint
├── README.md                          Ce fichier (setup technique)
├── .sqlfluff                          Config linter SQL
├── .pre-commit-config.yaml            Hooks Git
├── .github/workflows/                 CI GitHub Actions
│   └── dbt_ci.yml
│
├── dbt/                               Projet dbt
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml.example           Template — à copier vers ~/.dbt/
│   ├── models/
│   │   ├── staging/                   Vues staging (view + 2 incrementals)
│   │   ├── intermediate/              CTEs éphémères
│   │   └── marts/
│   │       ├── core/                  Dimensions et faits centraux
│   │       ├── finance/               Marts métier finance
│   │       └── analytics/             KPIs et analyses
│   ├── snapshots/                     SCD2 comptes
│   ├── macros/                        Macros custom + generic tests
│   ├── tests/                         Tests singuliers
│   ├── unit_tests/                    Tests unitaires (dbt 1.8+)
│   └── analyses/                      Analyses ad-hoc
│
├── scripts/
│   ├── python/
│   │   ├── generate_data.py           Générateur de données synthétiques
│   │   └── requirements.txt
│   └── snowflake/
│       ├── 01_setup_infrastructure.sql   Base, schémas, warehouses, rôles
│       ├── 02_ddl_raw_tables.sql         DDL avec clustering keys
│       ├── 03_stage_and_copy.sql         Stage + COPY INTO
│       ├── 04_performance_features.sql   SO, MV, clustering info
│       └── 05_audit_tables.sql           Tables d'audit pour hooks
│
├── orchestration/
│   ├── README.md
│   ├── airflow_dag_fintrack.py        DAG production (horaire + tests hebdo)
│   └── sql/copy_into_raw.sql          COPY incrémental appelé par le DAG
│
└── docs/                              Livrables de la mission
    ├── PERF_LOG.md                    Mesures avant/après, Query Profile, décisions
    ├── DESIGN.md                      Décisions d'architecture
    ├── CONTRACTS.md                   Contrats et versioning des modèles publics
    ├── RAPPORT_FINAL.md               Rapport final (gains, risques, ×10)
    └── elementary/elementary_report.html
```

---

## Prérequis

- Un compte **Snowflake** (trial gratuit ou payant — le trial suffit pour scale S/M)
- **Python 3.11+**
- **dbt-snowflake >= 1.10.5** (dbt Core, utilisé en CI) ou **dbt Fusion 2.x** (poste local) — microbatch, `schema` sur les snapshots et syntaxe `arguments:` des tests requièrent ces versions
- **SnowSQL** pour uploader les CSV vers le stage interne
- **Git + GitHub** (pour la CI)

---

## Setup en 8 étapes

### 1. Cloner et créer un environnement virtuel

```bash
git clone <votre-repo> fintrack-perf-scale
cd fintrack-perf-scale
python -m venv .venv
source .venv/bin/activate
pip install -r scripts/python/requirements.txt
pip install "dbt-core~=1.10.5" "dbt-snowflake~=1.10.0" sqlfluff sqlfluff-templater-dbt
pre-commit install
```

### 2. Provisionner Snowflake

Depuis Snowsight (rôle ACCOUNTADMIN), exécutez :

```bash
scripts/snowflake/01_setup_infrastructure.sql
```

Puis attribuez les rôles à votre utilisateur (dernières lignes du script).

### 3. Configurer dbt

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Éditer les variables d'environnement :
export SNOWFLAKE_ACCOUNT=xy12345.eu-west-1
export SNOWFLAKE_USER=votre_user
export SNOWFLAKE_PASSWORD=***
```

Vérifier :

```bash
cd dbt
dbt debug --target dev
dbt deps
```

### 4. Créer les tables RAW

```bash
# Dans Snowsight, exécuter :
scripts/snowflake/02_ddl_raw_tables.sql
scripts/snowflake/05_audit_tables.sql
```

### 5. Générer et charger les données

```bash
# Générer (choisir un scale)
python scripts/python/generate_data.py --scale M --output data/raw/

# Uploader vers le stage Snowflake (SnowSQL)
snowsql -q "USE ROLE FINTRACK_INGESTION_ROLE; USE WAREHOUSE WH_INGESTION; \
           USE DATABASE FINTRACK_PROD; USE SCHEMA RAW; \
           PUT file://data/raw/*.csv.gz @FINTRACK_STAGE AUTO_COMPRESS=FALSE;"

# COPY INTO
snowsql -f scripts/snowflake/03_stage_and_copy.sql
```

### 5 bis. Clone RAW de développement

En target `dev`, les sources lisent `FINTRACK_DEV.DEV_<USER>_RAW`, un clone zero-copy de la RAW de prod. On peut y simuler des changements de statut ou de KYC sans toucher à la prod. Le clone ne coûte rien tant qu'il n'est pas modifié.

```sql
USE ROLE FINTRACK_DEV_ROLE;
CREATE SCHEMA IF NOT EXISTS FINTRACK_DEV.DEV_<USER>_RAW;
CREATE OR REPLACE TABLE FINTRACK_DEV.DEV_<USER>_RAW.raw_transactions CLONE FINTRACK_PROD.RAW.raw_transactions;
-- idem pour raw_tenants, raw_categories, raw_fx_rates, raw_comptes,
-- raw_virements, raw_titulaires, raw_compte_titulaires
```

Pour lire directement la prod depuis dev : `--vars '{raw_database: FINTRACK_PROD, raw_schema: RAW}'`.

### 6. Premier `dbt build`

```bash
cd dbt

# Build complet (initial full-refresh)
dbt build --target dev --exclude tag:todo

# Vérifier les modèles créés
dbt list --target dev --resource-type model
```

### 7. Vérifier les tests

```bash
dbt test --target dev --exclude tag:todo
dbt test --select unit_test:*   # dbt 1.8+
```

### 8. Générer et servir la doc

```bash
dbt docs generate --target dev
dbt docs serve --port 8080
```

---

## Presets de volume du générateur

| Scale | Transactions | Comptes | Durée génération | RAM |
|-------|--------------|---------|------------------|-----|
| XS    |         10k  |    200  |   ~30 sec        | <1 Go |
| S     |        100k  |     2k  |   ~3 min         | <1 Go |
| M     |          1M  |    20k  |   ~10 min        | 2 Go  |
| L     |         10M  |   200k  |   ~1 h           | 4 Go  |
| XL    |         50M  |     1M  |   ~5 h           | 8 Go  |
| XXL   |        100M  |     2M  |   ~10 h          | 16 Go |

**Recommandation :** commencez sur `M` pour valider votre pipeline, montez sur `L` pour benchmarker les optimisations du Sprint 3, `XL` uniquement si vous avez le crédit Snowflake.

---

## Convention Git

- `main` — production, protégée
- `staging` — pré-production, déployée depuis les PR mergées
- `feat/<sprint>-<us>-<slug>` — features (ex : `feat/sprint3-us31-incremental-merge`)
- `fix/<ticket>-<slug>` — corrections

Chaque PR doit :
- Passer la CI (lint + slim build)
- Contenir au moins un test si un modèle est ajouté/modifié
- Documenter les changements de matérialisation dans `docs/DESIGN.md`

---

## Commandes utiles

```bash
# Build seulement les modèles modifiés depuis prod
dbt build --select state:modified+ --defer --state target-prod/

# Run un tag précis
dbt run --select tag:staging
dbt run --select tag:critical

# Compiler sans exécuter (utile pour debug templating)
dbt compile --select fct_transactions

# Inspecter le lineage
dbt list --select +fct_transactions+

# Nettoyer les schémas CI après une PR (refuse tout schéma sans préfixe CI_)
dbt run-operation drop_ci_schema --target ci --args "{schema_name: CI_42}"

# Tests complets (hors fenêtre incrémentale) — CI et job hebdomadaire
dbt test --vars '{tests_complets: true}'

# Rejouer une fenêtre incrémentale plus large après un incident
dbt build --select tag:incremental --vars '{incremental_lookback_days: 7}'

# Prototype microbatch (désactivé par défaut)
dbt run -s fct_transactions_microbatch --vars '{enable_microbatch: true}' \
  --event-time-start 2024-12-01 --event-time-end 2025-01-01

# Rapport Elementary (CLI : pip install "elementary-data[snowflake]",
# profil `elementary` pointant sur le schéma <target>_elementary)
edr report --file-path docs/elementary/elementary_report.html
```

---

## Debug perf Snowflake

- **Voir un plan de requête :** Snowsight → History → cliquer la query → onglet Profile
- **Coûts par query_tag :**
  ```sql
  SELECT query_tag, SUM(credits_used_cloud_services)
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
  GROUP BY query_tag ORDER BY 2 DESC;
  ```
- **Vérifier le clustering :**
  ```sql
  SELECT SYSTEM$CLUSTERING_INFORMATION('RAW.raw_transactions');
  ```

---

## Support

- Voir `MISSION.md` pour le contexte et les livrables attendus
- Erreur ? Vérifier d'abord : rôle actif, warehouse repris, base courante, fichiers uploadés
- Coûts qui dérivent ? Auto-suspend n'a pas fonctionné, ou une query hors du query_tag

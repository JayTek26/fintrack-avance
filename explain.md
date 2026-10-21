# EXPLAIN — Tout ce qui a été fait sur le projet, où, et comment ça marche

Ce document explique chaque action réalisée pendant la session du 20/10/2026 (sprints 3 à 6), le fichier où elle se trouve, et le fonctionnement détaillé du code.

- Les **chiffres** (temps, octets, crédits) sont dans `docs/PERF_LOG.md`.
- Les **décisions d'architecture** sont dans `docs/DESIGN.md`.
- Ici, on explique le *pourquoi* et le *comment* de chaque ligne importante.

---

## Sommaire

0. [Point de départ : l'état trouvé](#0-point-de-départ--létat-trouvé)
0 bis. [Sprints 1 et 2 — réalisés avant la session](#0-bis-sprints-1-et-2--réalisés-avant-la-session)
1. [Bug n°1 : `_loaded_at` NULL et le clone de développement](#1-bug-n1--_loaded_at-null-et-le-clone-de-développement)
2. [Bug n°2 : la conversion FX à l'envers](#2-bug-n2--la-conversion-fx-à-lenvers)
3. [Bug n°3 : les tests unitaires ne tournaient pas](#3-bug-n3--les-tests-unitaires-ne-tournaient-pas)
4. [Sprint 3 — Incrémental merge](#4-sprint-3--incrémental-merge-us-31)
5. [Sprint 3 — Microbatch](#5-sprint-3--microbatch-us-32)
6. [Sprint 3 — Performance : tests, query tags, Search Optimization, clustering](#6-sprint-3--performance-us-33)
7. [Sprint 4 — SCD2 et FX historisé](#7-sprint-4--scd2-et-fx-historisé)
8. [Sprint 5 — CI GitHub Actions](#8-sprint-5--ci-github-actions-us-51)
9. [Sprint 5 — DAG Airflow](#9-sprint-5--dag-airflow-us-52)
10. [Sprint 6 — Cohortes et anomalies](#10-sprint-6--cohortes-et-anomalies-us-61)
11. [Sprint 6 — Contrats et versions](#11-sprint-6--contrats-et-versions-us-62)
12. [Sprint 6 — Elementary](#12-sprint-6--elementary-us-63)
13. [Fichiers de configuration du dépôt](#13-fichiers-de-configuration-du-dépôt)
14. [Simulations exécutées dans Snowflake (hors dépôt)](#14-simulations-exécutées-dans-snowflake-hors-dépôt)
15. [Git : branche, commits, push](#15-git--branche-commits-push)
16. [Ce qui n'a pas été fait et pourquoi](#16-ce-qui-na-pas-été-fait-et-pourquoi)
17. [Index : fichier par fichier](#17-index--fichier-par-fichier)

---

## 0. Point de départ : l'état trouvé

État du projet au début de la session :

| Sprint | État trouvé |
|---|---|
| 1 | Fait : infra Snowflake, chargement scale L (10M transactions), PERF_LOG partiellement rempli |
| 2 | Fait : `dim_categories` récursif, `fct_virements` double écriture, bridge titulaires |
| 3 | En cours : squelettes incrémentaux en place, `fct_transactions_microbatch.sql` **vide**, aucune mesure |
| 4 | Squelette `dim_comptes_scd2`, snapshot jamais lancé avec des changements |
| 5 | DAG Airflow en squelette, **workflow CI absent** (annoncé « fourni » mais inexistant) |
| 6 | `mart_cohortes_retention` et `mart_transactions_anomalies` en squelette (valeurs en dur), contrat désactivé, Elementary commenté |

Les diagnostics ont été faits avec :
- `dbt show --inline "<sql>"` ;
- un petit script SnowSQL du scratchpad, qui lance `snowsql` avec les variables `SNOWFLAKE_*` de l'environnement ;
- la lecture de `dbt/logs/dbt.log` pour reconstituer l'historique des runs.

---

## 0 bis. Sprints 1 et 2 — réalisés avant la session

Ces deux sprints étaient terminés quand la session a commencé. Cette section explique ce qu'ils contiennent et comment ça fonctionne, d'après le code et `docs/PERF_LOG.md`. Les corrections apportées ensuite sont signalées.

### Sprint 1 — Infrastructure et chargement

**Objectif :** préparer Snowflake selon les bonnes pratiques (workloads séparés, coûts maîtrisés) et charger un volume représentatif.

**`scripts/snowflake/01_setup_infrastructure.sql`** — exécuté en ACCOUNTADMIN :
- **Bases et schémas :** `FINTRACK_PROD` avec un schéma par couche :
  - `RAW` : brut ;
  - `STAGING` : nettoyé ;
  - `MARTS_CORE` / `MARTS_FINANCE` / `MARTS_ANALYTICS` : métier ;
  - `AUDIT` : journaux ;
  - `SNAPSHOTS` : historique SCD2.
  
  Plus une base séparée `FINTRACK_DEV` pour le développement.
- **Resource monitor `RM_FINTRACK_MONTHLY`** : 100 crédits/mois.
  ```sql
  TRIGGERS ON 75 PERCENT DO NOTIFY            -- alerte
           ON 90 PERCENT DO SUSPEND           -- suspend quand les requêtes en cours finissent
           ON 100 PERCENT DO SUSPEND_IMMEDIATE -- coupe tout
  ```
- **5 warehouses**, un par type de charge :

  | Warehouse | Taille | Clusters | Auto-suspend | Usage |
  |---|---|---|---|---|
  | WH_INGESTION | LARGE (repassé XSMALL depuis) | 1 | 60 s | COPY INTO |
  | WH_TRANSFORM | MEDIUM | 1 → 3 | 60 s | dbt |
  | WH_REPORTING | SMALL | 1 → 5, ECONOMY | 300 s | dashboards |
  | WH_ADHOC | XSMALL | 1 | 60 s | exploration |
  | WH_DEV | XSMALL | 1 | 60 s | développement |

  - `AUTO_SUSPEND` éteint le warehouse après N secondes d'inactivité : on ne paie que le temps actif.
  - Le multi-cluster ajoute des copies du warehouse quand des requêtes font la queue (concurrence BI).
  - REPORTING garde 300 s pour conserver le cache local chaud entre deux clics de dashboard.
- **5 rôles**, selon une matrice RACI au moindre privilège :
  - `INGESTION` : écrit dans RAW ;
  - `TRANSFORM` : lit RAW, écrit dans STAGING, les MARTS, SNAPSHOTS et AUDIT ;
  - `REPORTING` : lit les MARTS uniquement ;
  - `ADHOC` : lit toute la prod ;
  - `DEV` : tous les droits sur FINTRACK_DEV, lecture sur la prod.
  
  Les `GRANT ... ON FUTURE TABLES` donnent automatiquement les droits sur les tables créées plus tard.

**`scripts/snowflake/02_ddl_raw_tables.sql`** — tables RAW :
- `raw_transactions` reproduit un vrai core banking avec environ 80 colonnes : timestamps multiples, montants et devises, marchand, carte, device, AML et fraude, rapprochement, métadonnées.
- Chaque table a une colonne `_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()`, l'heure d'arrivée dans RAW.
- Des clés de clustering sont posées : `(tenant_id, DATE_TRUNC('MONTH', date_transaction))` sur les transactions. Constat du PERF_LOG : un `COPY INTO` échoue sur une table dont la clé contient `DATE_TRUNC`, donc le clustering a été posé après le chargement.

**`scripts/python/generate_data.py`** — génération des données synthétiques :
- `--scale XS … XXL` choisit le volume (L = 10M transactions, 200k comptes, 12 tenants).
- Les tenants viennent d'une liste réaliste (`TENANTS_SEED` : BNP-FR, CA-FR, ING-NL, DB-DE, HSBC-UK…).
- Les transactions sont générées par lots avec numpy (statuts tirés selon des poids : environ 92 % `validee`) et écrites directement en CSV gzip.

**`scripts/snowflake/03_stage_and_copy.sql`** — chargement :
1. `CREATE FILE FORMAT ff_csv_gz` (CSV gzip, en-tête, NULL si vide).
2. `CREATE STAGE fintrack_stage` (stage interne).
3. `PUT file://data/raw/*.csv.gz @fintrack_stage`, côté client SnowSQL.
4. `COPY INTO raw_<table> FROM @fintrack_stage/... ON_ERROR = 'CONTINUE'` : une ligne invalide est ignorée et comptée au lieu de faire échouer tout le fichier.

**Résultats (PERF_LOG) :**
- 3,18 Go compressés ;
- PUT en 2 h 15 : le débit montant du portable, ≈ 3 Mbit/s, est le goulot ;
- COPY en 2 min 44 (≈ 66 000 lignes/s pour les transactions), 0 erreur ;
- profondeur de clustering de RAW au 19/10 : 6,86.

**Corrigé pendant la session :** le COPY laissait `_loaded_at` vide (section 1 ci-dessous).

### Sprint 2 — Modélisation core et double écriture

**Objectif :** compléter la modélisation Kimball (dimensions, faits) et gérer les comptes joints.

**`dbt/models/marts/core/dim_categories.sql`** — hiérarchie récursive :
```sql
with recursive categories as ( select ... from {{ ref('stg_categories') }} ),
hierarchie as (
    -- ANCRAGE : les racines (pas de parent, ou parent inactif donc absent de stg)
    select c.*, 1 as niveau, c.categorie_id as id_racine,
           cast(c.nom_categorie as varchar(4000)) as chemin_complet
    from categories c
    left join categories p on c.categorie_parent_id = p.categorie_id
    where p.categorie_id is null

    union all

    -- RÉCURSION : les enfants des nœuds déjà trouvés, un niveau plus bas
    select c.*, h.niveau + 1, h.id_racine, h.chemin_complet || ' > ' || c.nom_categorie
    from categories c
    inner join hierarchie h on c.categorie_parent_id = h.categorie_id
    where h.niveau < 10      -- garde-fou contre les cycles
)
```
- La requête s'appelle elle-même : chaque passage ajoute les enfants du niveau précédent, jusqu'à ce qu'il n'y en ait plus.
- Résultat : pour chaque catégorie, son `niveau`, sa racine (`id_racine`) et son chemin lisible (`Loisirs > Restaurant > Fast-food`).
- `cast(... as varchar(4000))` : sans lui, la colonne prendrait la taille du nom racine et les chemins longs seraient tronqués.
- Testé par `unit_tests/unit_dim_categories.yml` (3 niveaux).

**`dbt/models/marts/core/fct_virements.sql`** — double écriture :
```sql
leg_sortante as ( select virement_id, compte_source_id as compte_id, compte_dest_id as contrepartie_compte_id,
                         'sortant' as virement_leg, -1 * abs(montant) as montant_signe, ... from virements ),
leg_entrante as ( select virement_id, compte_dest_id   as compte_id, compte_source_id as contrepartie_compte_id,
                         'entrant' as virement_leg,      abs(montant) as montant_signe, ... from virements )
select * from leg_sortante union all select * from leg_entrante
```
- Un virement exécuté devient 2 lignes : le débit sur le compte source, le crédit sur le compte destination. C'est le principe comptable de la partie double : le mouvement total vaut zéro.
- Chaque compte voit donc ses virements comme des mouvements ordinaires, ce qui sert au calcul des soldes dans `mart_solde_journalier`.
- Modèle incrémental (merge sur la clé `virement_id, virement_leg`).
- Tests :
  - `assert_virements_double_ecriture` : exactement 2 lignes par virement, de somme nulle ;
  - unicité `(virement_id, virement_leg)`.
- **Modifié pendant la session :** ajout de `montant_eur` / `montant_signe_eur` au taux du jour (section 2) et réécriture du test unitaire (section 3).

**US 2.2 — bridge comptes ↔ titulaires**

1. **Générateur** (`generate_titulaires()` dans `generate_data.py`) :
   - environ 85 % des comptes ont 1 titulaire, 15 % sont joints (2 ou 3 titulaires) ;
   - parts égales, le premier titulaire absorbe l'arrondi pour que le total fasse exactement 100 ;
   - pour environ 5 % des comptes joints, le dernier co-titulaire est **remplacé** en cours de vie : l'ancien lien reçoit une `date_fin`, et un nouveau lien s'ouvre à la même date avec la même part ;
   - intervalles semi-ouverts `[date_debut, date_fin[` : un jour donné, un seul des deux liens est actif.
   
   Fichiers produits : `raw_titulaires` (personnes) et `raw_compte_titulaires` (liens).
2. **DDL** des deux tables ajoutée au script 02.
3. **Staging** : `stg_titulaires.sql`, `stg_compte_titulaires.sql`. Tests dans `_staging.yml` : clés, relations vers comptes et titulaires, rôle dans (`PRINCIPAL`, `CO_TITULAIRE`), part entre 0 et 100.
4. **Bridge** (`dbt/models/marts/core/bridge_comptes_titulaires.sql`) :
   ```sql
   select
       {{ dbt_utils.generate_surrogate_key(['compte_id', 'titulaire_id', 'date_debut']) }} as bridge_key,
       compte_id, titulaire_id, role_titulaire,
       round(part_pct / 100.0, 4)     as allocation_factor,   -- 50 % → 0.5
       date_debut, date_fin,
       (role_titulaire = 'PRINCIPAL') as is_primary,
       (date_fin is null)             as is_current
   from {{ ref('stg_compte_titulaires') }}
   ```
   - `allocation_factor` sert à ventiler : la dépense d'une personne = `sum(montant_eur * allocation_factor)` sur ses comptes, en joignant sur la période active.
   - `bridge_key` : identifiant unique d'un lien sur une période (un même couple compte/titulaire peut revenir plus tard).
   - Test `assert_bridge_allocation_somme_a_1` : pour chaque compte et chaque date de début de lien, la somme des facteurs des liens actifs ce jour-là doit valoir 1 (tolérance 0,01).

**Livrable du sprint :** `dbt test --exclude tag:todo` vert (dernier build complet du 20/10 au matin : 109 nœuds, 0 erreur, d'après `dbt/logs/dbt.log`).

**Corrigé pendant la session :** les tests unitaires de ce sprint n'étaient pas exécutés (dossier non déclaré à dbt), et celui de `fct_virements` était faux (section 3).

---

## 1. Bug n°1 : `_loaded_at` NULL et le clone de développement

### Constat
```sql
select count(*), count(_loaded_at) from FINTRACK_PROD.RAW.RAW_TRANSACTIONS;
-- 10 000 000 | 0
```
`_loaded_at` était NULL sur les 10M transactions, et aussi sur comptes, virements, tenants, catégories et FX.

Or tous les modèles incrémentaux filtrent ainsi :
```sql
where _loaded_at >= (select dateadd('day', -7, max(_loaded_at)) from {{ this }})
```
`max(NULL)` = NULL, et `x >= NULL` est toujours faux. **Chaque run incrémental traitait donc 0 ligne, sans erreur.**

### Cause
Script `scripts/snowflake/03_stage_and_copy.sql`, version d'origine :
- le file format utilise `SKIP_HEADER = 1`, donc un chargement **par position de colonne** ;
- la table a 1 colonne de plus que le CSV (`_loaded_at DEFAULT CURRENT_TIMESTAMP()`) ;
- avec `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE`, Snowflake remplit la colonne manquante avec **NULL**. Le `DEFAULT` ne s'applique qu'aux colonnes *absentes de la liste des colonnes du COPY*, pas aux colonnes manquantes du fichier.

### Correctif pour les chargements futurs
**Fichier : `scripts/snowflake/03_stage_and_copy.sql`**

```sql
CREATE OR REPLACE FILE FORMAT ff_csv_gz
    TYPE = CSV
    ...
    PARSE_HEADER = TRUE              -- lit la 1re ligne comme noms de colonnes
    ...
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    TRIM_SPACE = TRUE;

COPY INTO raw_transactions
FROM @fintrack_stage/raw_transactions.csv.gz
FILE_FORMAT = (FORMAT_NAME = ff_csv_gz)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE                      -- colonne CSV → colonne table, par nom
INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)   -- remplit _loaded_at avec l'heure du COPY
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;
```

- `PARSE_HEADER = TRUE` remplace `SKIP_HEADER = 1` : les deux sont incompatibles.
- `MATCH_BY_COLUMN_NAME` charge par nom de colonne. C'est aussi plus robuste si l'ordre des colonnes du CSV change.
- `INCLUDE_METADATA` n'est autorisé qu'avec `MATCH_BY_COLUMN_NAME`. Il injecte une métadonnée Snowflake dans une colonne : ici l'heure de début du scan du fichier.

Ajouts dans ce même fichier :
- les `COPY` de `raw_titulaires` et `raw_compte_titulaires` (Sprint 2), qui manquaient ;
- un contrôle final `COUNT(*) - COUNT(_loaded_at)`, qui doit valoir 0.

**Validation :** testé sur un vrai fichier (`raw_tenants.csv.gz`) dans un stage temporaire de dev → 8 lignes, 8 `_loaded_at` remplis.

### Les données déjà chargées
- **En prod : non corrigées.** Le mode auto de Claude Code a refusé l'UPDATE sur `FINTRACK_PROD.RAW` (donnée prod).
- La requête à lancer est documentée dans `docs/PERF_LOG.md` :
  ```sql
  UPDATE raw_transactions SET _loaded_at = '2026-10-14 08:55:19' WHERE _loaded_at IS NULL;
  ```
  L'heure vient de `SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY` (COPY du 14/10, heure du compte).

### Le clone de développement (méthode sûre adoptée)
**Fait dans Snowflake** (pas de fichier dans le dépôt) avec `FINTRACK_DEV_ROLE` :
```sql
create schema FINTRACK_DEV.DEV_JAY_RAW;
create or replace table FINTRACK_DEV.DEV_JAY_RAW.raw_transactions clone FINTRACK_PROD.RAW.raw_transactions;
-- … idem pour les 8 tables raw_*
```
Un **clone zero-copy** est une nouvelle table qui pointe vers les mêmes micro-partitions que l'original. Il ne coûte rien en stockage tant qu'on ne le modifie pas, et une modification n'affecte jamais l'original. Le backfill de `_loaded_at` a ensuite été fait dans ce clone : 10M lignes en 27 s sur XSMALL.

**Fichier : `dbt/models/staging/_sources.yml`.** Les sources pointent vers le clone en dev :
```yaml
database: "{{ var('raw_database', 'FINTRACK_DEV' if target.name == 'dev' else 'FINTRACK_PROD') }}"
schema:   "{{ var('raw_schema',   target.schema ~ '_RAW' if target.name == 'dev' else 'RAW') }}"
```
- En target `dev` : `FINTRACK_DEV.DEV_JAY_RAW` (`target.schema` = `DEV_JAY`, plus `_RAW`).
- En `ci` / `prod` : `FINTRACK_PROD.RAW`.
- `var(...)` permet de forcer n'importe quelle base : `--vars '{raw_database: FINTRACK_PROD, raw_schema: RAW}'`.

---

## 2. Bug n°2 : la conversion FX à l'envers

### Constat
Comparaison des taux de `raw_fx_rates` avec les montants :
- `raw_fx_rates` contient `devise_source = 'EUR'`, `devise_cible = 'JPY'`, `taux ≈ 173`. Cela signifie **1 EUR = 173 JPY**.
- L'ancien `fct_transactions` faisait `montant * taux`. Un achat de 1 000 JPY valait donc 173 000 € au lieu de 5,78 €, soit une erreur d'un facteur 173² ≈ 30 000.
- Le `montant_eur` du core banking n'est pas fiable non plus. Le générateur (`generate_data.py`, ligne ~358) utilise un taux aléatoire entre 0,85 et 1,30 : le ratio vaut ≈ 1,075 pour toutes les devises.

### Correctif
**Fichier : `dbt/models/marts/core/fct_transactions.sql`**
```sql
round(b.montant / fx.taux, 2)::number(18, 2) as montant_eur,
b.montant_signe,
(sign(b.montant_signe) * round(b.montant / fx.taux, 2))::number(18, 2) as montant_signe_eur,
...
left join fx
    on  fx.date_cotation = b.jour_transaction     -- avant : cast(b.date_transaction as date)
    and fx.devise        = b.devise
```
- **Division** au lieu de multiplication.
- **Plus de repli** `coalesce(fx.taux, taux_change_applique, 1)` : si un taux manque, `montant_eur` est NULL et le test `not_null` échoue. On préfère une erreur visible à un montant faux.
- `montant_signe_eur` est **recalculé** (avant, il venait du `montant_eur` source, faux) : `sign()` renvoie −1 pour un débit, +1 pour un crédit.
- `::number(18,2)` fixe le type exact, ce qu'exige le contrat (section 11).

**Fichier : `dbt/models/marts/core/fct_virements.sql`**, même logique ajoutée :
```sql
virements_eur as (
    select v.*, round(v.montant / fx.taux, 2) as montant_eur
    from virements v
    left join fx on fx.date_cotation = v.date_virement::date and fx.devise = v.devise
),
```
Chaque leg porte `montant_eur` et `montant_signe_eur` (−abs pour la sortante, +abs pour l'entrante).

**Fichier : `dbt/models/marts/finance/mart_solde_journalier.sql`.** Le solde utilise désormais `sum(montant_signe_eur)` pour les virements. Avant, il additionnait des montants en USD, GBP et EUR comme s'ils étaient en EUR.

**Fichier : `dbt/models/intermediate/int_fx_rates_daily.sql`**
- La grille de dates s'arrêtait au 01/01/2025, alors que 88 % des virements sont datés de 2025-2026. Ils n'auraient eu aucun taux.
  ```sql
  end_date="dateadd('day', 1, current_date())"
  ```
- Ajout de `date_cotation_source` : la date de la cotation réellement utilisée par le forward-fill.
  ```sql
  last_value(s.date_cotation ignore nulls) over (
      partition by gc.devise_cible order by gc.date_cotation
      rows between unbounded preceding and current row
  ) as date_cotation_source
  ```
  `last_value(... ignore nulls)` sur une fenêtre « du début jusqu'à la ligne courante » donne la dernière valeur non nulle connue : c'est le *last observation carried forward*.

---

## 3. Bug n°3 : les tests unitaires ne tournaient pas

**Fichier : `dbt/dbt_project.yml`**
```yaml
test-paths:      ["tests", "unit_tests"]   # avant : ["tests"]
```
dbt ne lit les fichiers `.yml` que dans les dossiers déclarés dans un `*-paths`. Le dossier `unit_tests/` n'y figurait pas, donc les 6 tests unitaires étaient ignorés.

Une fois activés, deux tests étaient faux :

**`dbt/unit_tests/unit_fct_virements.yml`**
- Colonnes inexistantes : `compte_destination_id` au lieu de `compte_dest_id`, `sens: 'SORTANT'` au lieu de `virement_leg: 'sortant'`.
- Types incohérents : `virement_id: 'V1'` alors que la colonne est un entier.
- Réécrit avec 2 virements en entrée : un `execute` en USD et un `rejete`. On attend 2 legs pour le premier, montant signé −100 / +100, et 80 € au taux fictif de 1,25. Le rejeté doit être ignoré. L'input `int_fx_rates_daily` est mocké.

**`dbt/unit_tests/unit_stg_transactions_incremental.yml`.** Teste le cœur de l'US 3.1 :
```yaml
overrides:
  macros:
    is_incremental: true          # on force la branche incrémentale du modèle
given:
  - input: source('fintrack_raw', 'raw_transactions')
    rows:
      - {transaction_id: 1, statut: 'en_attente', _loaded_at: '2025-01-31 10:00:00', ...}  # ancienne version
      - {transaction_id: 1, statut: 'validee',    _loaded_at: '2025-02-01 10:00:00', ...}  # nouvelle version
      - {transaction_id: 2, ..., _loaded_at: '2025-01-01 10:00:00'}                        # hors fenêtre
  - input: this                   # contenu actuel de la table cible
    rows:
      - {transaction_id: 1, statut: 'en_attente', _loaded_at: '2025-01-31 10:00:00'}
expect:
  rows:
    - {transaction_id: 1, statut: 'validee', montant_signe: -10}
```
On vérifie trois choses à la fois :
- le filtre de lookback (la transaction 2 est exclue) ;
- la déduplication (une seule ligne pour la transaction 1) ;
- le choix de la version la plus récente (`validee`).

---

## 4. Sprint 3 — Incrémental merge (US 3.1)

### `dbt/models/staging/stg_transactions.sql`

```sql
{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        merge_update_columns=[ 'statut', 'statut_precedent', ..., 'aml_flag', 'aml_score', ...,
                               'is_reconciled', 'reconciliation_batch_id', ..., '_loaded_at'],
        cluster_by=['tenant_id', "date_trunc('month', date_transaction)"],
        on_schema_change='append_new_columns',
        event_time='date_transaction',
        ...
    )
}}
```
- `merge` + `unique_key` : dbt génère un `MERGE INTO cible USING (nouvelles lignes) ON cible.transaction_id = source.transaction_id`, avec `WHEN MATCHED THEN UPDATE` et `WHEN NOT MATCHED THEN INSERT`.
- `merge_update_columns` limite le `UPDATE` aux colonnes qui peuvent changer : statut, rejets, AML, rapprochement. Les colonnes immuables (montant, date, marchand…) ne sont pas réécrites.
- `on_schema_change='append_new_columns'` : une nouvelle colonne apparue en source est ajoutée à la table, sans rebuild.
- `event_time` sert au microbatch aval (section 5) ; il est sans effet sur ce merge.

Filtre de la source :
```sql
with source as (
    select * from {{ source('fintrack_raw', 'raw_transactions') }}
    {% if is_incremental() %}
        where _loaded_at >= (
            select dateadd('day', -{{ var('incremental_lookback_days', 7) }}, max(_loaded_at))
            from {{ this }}
        )
    {% elif target.name == 'ci' %}
        where date_transaction >= (
            select dateadd('day', -{{ var('ci_lookback_days', 30) }}, max(date_transaction))
            from {{ source('fintrack_raw', 'raw_transactions') }}
        )
    {% endif %}
),
```
- `is_incremental()` est vrai quand la table existe déjà et qu'on n'est pas en `--full-refresh`.
- **Le watermark est `max(_loaded_at)` de la cible**, moins une marge de N jours (le lookback).
- **En CI, sur un build complet**, on ne construit que les 30 derniers jours de données : la CI vérifie la logique, pas la volumétrie, et reste sous 10 minutes. L'ordre `if is_incremental / elif ci` est important : dans l'autre sens, la branche CI court-circuitait le test unitaire incrémental (bug rencontré et corrigé).

La déduplication (déjà présente, conservée) :
```sql
deduped as (
    select * from source
    qualify row_number() over (partition by transaction_id order by _loaded_at desc, updated_at desc) = 1
),
```
Si RAW reçoit une nouvelle ligne à chaque changement de statut (append-only), la fenêtre peut contenir deux versions d'une même transaction. Un `MERGE` avec deux lignes source pour une même clé est non déterministe, et Snowflake peut lever une erreur. `QUALIFY` filtre directement sur le résultat de la window function : on garde la version la plus récente.

### `dbt/models/marts/core/fct_transactions.sql`

Mêmes principes, avec en plus :
- le lookback, qui était codé en dur à `-7`, passe en `var('incremental_lookback_days')` ;
- `merge_update_columns` est étendu : `date_settlement`, `reconciliation_batch_id`, `is_flagged_for_review`, `montant_signe_eur`, `_loaded_at`.

**Problème découvert : le grain.** Le modèle filtre `where statut in ('validee', 'en_attente')`. Une transaction déjà en table en `en_attente` qui passe `rejetee` n'est plus renvoyée par le SQL. Elle n'arrive donc pas dans le MERGE et reste **figée en `en_attente`** pour toujours.

**Fichier : `dbt/macros/purge_transactions_hors_perimetre.sql`**, appelé en post-hook :
```sql
delete from {{ this }} as tgt
using (
    select transaction_id
    from {{ ref('stg_transactions') }}
    where _loaded_at >= (select dateadd('day', -{{ var('incremental_lookback_days', 7) }}, max(_loaded_at))
                         from {{ ref('stg_transactions') }})
      and (statut not in ('validee', 'en_attente') or is_reversal)
) as hors_perimetre
where tgt.transaction_id = hors_perimetre.transaction_id
```
Après le merge, ce hook supprime du fait les transactions de la fenêtre qui sont sorties du périmètre (rejet, annulation, reversal). Sur une table neuve, il ne supprime rien, donc il est sans danger au premier run.

Ordre des post-hooks :
```sql
post_hook=[
    "{{ purge_transactions_hors_perimetre() }}",
    "{{ ensure_search_optimization(['external_transaction_id']) }}",
    "{{ log_incremental_run('fct_transactions', 'post') }}"
]
```

### `dbt/dbt_project.yml` — lookback
```yaml
vars:
  incremental_lookback_days: 1    # avant : 7
```
`_loaded_at` est l'heure d'ingestion : elle ne fait qu'augmenter, et une transaction modifiée est rechargée avec un **nouveau** `_loaded_at`. Une marge de 1 jour couvre les chargements concurrents ou rejoués.

Effet mesuré : sur `fct_transactions`, 15 110 lignes mises à jour par run au lieu de 91 480 (−83 %).

### Vérification du comportement
Faite dans le clone dev (section 14) :

| Changement simulé | Dans stg | Dans fct |
|---|---|---|
| 1 000 `en_attente → validee` | 1 000 en `validee` | 1 000 en `validee` |
| 200 `en_attente → rejetee` | 200 en `rejetee` | **0** (purgées) |
| 50 transactions de 2023 → `validee` | 50 | 50 |

---

## 5. Sprint 3 — Microbatch (US 3.2)

**Fichier : `dbt/models/marts/core/fct_transactions_microbatch.sql`** (vide au départ)

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='date_transaction',     -- colonne qui découpe les batchs
        batch_size='day',                  -- un batch = un jour
        lookback=3,                        -- retraite aussi les 3 jours précédents
        begin='2023-01-01',                -- début de l'historique
        concurrent_batches=true,           -- batchs en parallèle
        enabled=var('enable_microbatch', false),
        tags=[..., 'experimental']
    )
}}
```
Principe :
- dbt découpe le traitement en fenêtres `[jour, jour+1[` et lance une requête par fenêtre ;
- chaque fenêtre est **remplacée entièrement** (pas de clé de merge) ;
- toute `ref()` vers un modèle qui déclare `event_time` est **filtrée automatiquement** sur la fenêtre du batch.

Point technique : le modèle lit `ref('stg_transactions')` directement et refait les jointures comptes / catégories. Passer par l'éphémère `int_transactions_normalisees` empêchait le filtrage par batch : un modèle éphémère ne peut pas avoir d'`event_time`, et dbt avertissait « no input with event_time ».

`enabled=false` par défaut : sinon chaque `dbt build` lancerait ≈ 1 400 batchs (de 2023 à aujourd'hui). Lancement manuel :
```bash
dbt run -s fct_transactions_microbatch --vars '{enable_microbatch: true}' \
  --event-time-start 2024-12-01 --event-time-end 2025-01-01
```

Conclusion (détaillée dans PERF_LOG) : le microbatch est plus rapide de 7 s par run, mais il **ne voit pas les mises à jour sur des transactions anciennes** et il casse le clustering (profondeur 36). **Merge retenu.**

---

## 6. Sprint 3 — Performance (US 3.3)

### 6.1 Query tag par modèle
**Fichier : `dbt/macros/set_query_tag.sql`**
```sql
{% macro set_query_tag() -%}
    {% if target.type == 'snowflake' %}
        {% set tag = 'team=data-eng|project=fintrack-perf-scale|target=' ~ target.name %}
        {% if model is defined and model is mapping and model.get('resource_type') in ('model', 'snapshot', 'seed') %}
            {% set tag = tag ~ '|model=' ~ model['name'] %}
        {% endif %}
        {% set original_query_tag = get_current_query_tag() %}
        {% do run_query("alter session set query_tag = '" ~ tag ~ "'") %}
        {{ return(original_query_tag) }}
    {% endif %}
{%- endmacro %}
```
- Un macro du projet qui porte le même nom qu'un macro de l'adapter le **remplace**. Les materializations dbt-snowflake appellent `set_query_tag()` avant chaque modèle, donc chaque requête porte désormais `model=<nom>`. On peut agréger les coûts par modèle dans `QUERY_HISTORY` :
  ```sql
  regexp_substr(query_tag, 'model=([a-z0-9_]+)', 1, 1, 'e')
  ```
- Le macro retourne le tag précédent, comme celui de l'adapter, pour que `unset_query_tag(original_query_tag)` le restaure.
- Le filtre `resource_type` évite de taguer les hooks avec leur nom technique.

**`dbt/dbt_project.yml`** : les hooks passent de `{{ ... }}` à `{% do ... %}`.
```yaml
on-run-start:
  - "{% do set_query_tag() %}"
```
Comme le macro retourne maintenant une valeur, `{{ }}` l'aurait **imprimée comme du SQL** (erreur rencontrée : `unexpected 'target'`). `{% do %}` exécute sans rien rendre.

### 6.2 Tests fenêtrés : le plus gros gain
Constat : sur un run incrémental, les tests prenaient **66 % du temps warehouse**. Chaque `unique` / `not_null` / `relationships` scanne les 10M lignes.

**Fichier : `dbt/macros/get_where_subquery.sql`**, surcharge du macro interne de dbt utilisé par tous les tests génériques :
```sql
{% macro get_where_subquery(relation) -%}
    {%- set where = config.get('where') -%}
    {%- if where -%}
        {%- if '__fenetre_incrementale__' in where -%}
            {%- if var('tests_complets', false) -%}
                {%- set filtre = '1 = 1' -%}
            {%- else -%}
                {%- set jours = var('tests_lookback_days', var('incremental_lookback_days', 7)) -%}
                {%- set filtre = "_loaded_at >= (select dateadd('day', -" ~ jours ~ ", max(_loaded_at)) from " ~ relation ~ ")" -%}
            {%- endif -%}
            {%- set where = where | replace('__fenetre_incrementale__', filtre) -%}
        {%- endif -%}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {%- do return(filtered) -%}
    {%- else -%}
        {%- do return(relation) -%}
    {%- endif -%}
{%- endmacro %}
```
Fonctionnement :
- Dans le `.yml`, un test reçoit `config: where: "__fenetre_incrementale__"`.
- Au moment de compiler le test, dbt appelle `get_where_subquery(relation)` pour savoir sur quoi tester. Notre version remplace le placeholder par `_loaded_at >= max - N jours`. Le test ne lit donc que la fenêtre récente.
- `--vars '{tests_complets: true}'` remplace le filtre par `1 = 1` : test complet, pour la CI et le job hebdomadaire.

Exemple de SQL compilé :
```sql
select transaction_id as unique_field, count(*) as n_records
from (select * from FINTRACK_DEV.DEV_JAY_marts_core.fct_transactions
      where _loaded_at >= (select dateadd('day', -1, max(_loaded_at)) from ...fct_transactions)) dbt_subquery
where transaction_id is not null
group by transaction_id having count(*) > 1
```

Le placeholder est appliqué dans :
- `dbt/models/marts/core/_core.yml` (tous les tests de `fct_transactions`) ;
- `dbt/models/staging/_staging.yml` (`stg_transactions`) ;
- `dbt/models/staging/_sources.yml` (`raw_transactions`).

Pourquoi c'est sûr : le `MERGE` sur `unique_key` garantit déjà l'unicité hors de la fenêtre, et le run hebdomadaire complet rattrape le reste.

### 6.3 `store_failures` en prod uniquement
**`dbt/dbt_project.yml`**
```yaml
tests:
  +store_failures: "{{ (target.name == 'prod') | as_bool }}"
```
`store_failures` transforme chaque test en `CREATE TABLE AS` (pour stocker les lignes en échec) suivi d'un `SELECT`. C'est utile en prod pour l'audit, inutile en dev et en CI. `as_bool` convertit le texte rendu par Jinja en vrai booléen.

Effet combiné avec 6.2 : `dbt test` passe de 2 min 54 s à 58 s.

### 6.4 Search Optimization
Mesures (PERF_LOG, Query 1) : un lookup par `external_transaction_id` lisait 33 partitions sur 33. Avec la SO, il en lit 1 sur 33.

**Fichier : `dbt/macros/ensure_search_optimization.sql`**
```sql
{% macro ensure_search_optimization(equality_columns) %}
    {% if var('enable_search_optimization', target.name == 'prod') %}
        alter table {{ this }} add search optimization on equality({{ equality_columns | join(', ') }})
    {% else %}
        select 'search optimization désactivée sur ce target' as noop
    {% endif %}
{% endmacro %}
```
- Un `--full-refresh` **recrée** la table et perd donc la SO. Le post-hook la remet à chaque run.
- L'`ALTER` est idempotent : vérifié, le relancer sur une SO existante ne provoque pas d'erreur.
- Activé en prod seulement, car la SO a un coût de maintenance à chaque merge.

### 6.5 Clustering
- Mesures faites avec `SYSTEM$CLUSTERING_INFORMATION` (résultats dans PERF_LOG).
- Aucune clé modifiée dans le code : `(tenant_id, mois)` est conservée sur le fait.
- Deux recommandations documentées mais **non appliquées** : retirer la clé de `stg_transactions` et de RAW.

### 6.6 Query Profiles
Trois requêtes types (lookup par identifiant, dashboard tenant, relevé par IBAN) exécutées sans cache. Leurs plans ont été extraits avec :
```sql
select * from table(get_query_operator_stats('<query_id>'));
```
Cette fonction renvoie chaque nœud du plan (TableScan, Filter, Join…) avec les partitions lues, les octets et le % de temps : c'est l'équivalent texte de l'onglet *Profile* de Snowsight. Les query_id sont dans le PERF_LOG, pour aller faire les captures d'écran.

---

## 7. Sprint 4 — SCD2 et FX historisé

### 7.1 Snapshot
**Fichier : `dbt/snapshots/snapshot_comptes.sql`**
```sql
config(
    schema='snapshots',              -- avant : target_schema='snapshots'
    unique_key='compte_id',
    strategy='check',
    check_cols=['statut', 'kyc_level', 'aml_flag', 'email', 'type_compte', 'customer_segment', 'is_pep'],
    hard_deletes='invalidate'        -- avant : invalidate_hard_deletes=True (syntaxe dépréciée)
)
```
- La stratégie `check` compare les colonnes listées à chaque run. Si l'une change, dbt ferme la version courante (`dbt_valid_to` = maintenant) et en ouvre une nouvelle.
- `schema` (dbt ≥ 1.9) passe par `generate_schema_name`, alors que `target_schema` écrivait **toujours** dans `SNAPSHOTS`. Avec l'ancien réglage, le dev et la CI auraient écrit dans le même schéma que la prod.
- Le même changement est fait dans **`dbt/dbt_project.yml`** (`+schema: snapshots`) ; `require-dbt-version` passe à `>=1.9.0`.

### 7.2 Dimension SCD2
**Fichier : `dbt/models/marts/core/dim_comptes_scd2.sql`**
```sql
select
    {{ dbt_utils.generate_surrogate_key(['compte_id', 'date_debut_validite']) }} as compte_version_key,
    compte_id, ..., customer_segment, is_pep, risk_score,
    dbt_valid_from as date_debut_validite,
    dbt_valid_to   as date_fin_validite,
    (date_fin_validite is null) as is_current,
    row_number() over (partition by compte_id order by dbt_valid_from) as version_number
from {{ ref('snapshot_comptes') }}
```
- `generate_surrogate_key` calcule un hash MD5 de (compte, début de validité) : c'est l'identifiant unique de la version.
- `is_current` vaut vrai quand la version n'est pas fermée.
- `version_number` numérote les versions 1, 2, 3… par compte.

### 7.3 Contraintes SCD2
- **`dbt/tests/assert_scd2_une_version_courante.sql`** : `full outer join` entre le nombre de versions courantes par compte et la liste des comptes de la source. Le test échoue si un compte a plus d'une version courante, ou si un compte encore présent en source n'en a aucune.
- **`dbt/tests/assert_scd2_periodes_contigues.sql`** : avec `lead(date_debut_validite)`, il vérifie que la fin d'une version égale le début de la suivante (ni trou, ni chevauchement).
- **`dbt/models/marts/core/_core.yml`** : `unique` + `not_null` sur `compte_version_key`.

Validation :
- 3 changements simulés dans le clone (section 14) : compte 7 actif → suspendu → actif, compte 8 clôturé, compte 10 en KYC advanced ;
- 2 runs de snapshot ;
- résultat : 200 004 versions pour 200 000 comptes, tests verts.

### 7.4 Test FX sur 100 transactions
**Fichier : `dbt/tests/assert_fct_montant_eur_taux_du_jour.sql`**
```sql
with echantillon as (
    select ... from {{ ref('fct_transactions') }}
    where devise <> 'EUR'
    order by hash(transaction_id)     -- échantillon déterministe : même résultat à chaque run
    limit 100
),
recalcul as (
    select e.*, c.date_cotation as date_cotation_utilisee, round(e.montant / c.taux, 2) as montant_eur_attendu
    from echantillon e
    asof join cotations c
        match_condition (e.jour_transaction >= c.date_cotation)
        on e.devise = c.devise_cible
)
select * from recalcul
where montant_eur_attendu is null
   or abs(montant_eur - montant_eur_attendu) / nullif(abs(montant_eur_attendu), 0) > 0.0001
```
- **`ASOF JOIN`** (Snowflake) : pour chaque transaction, il prend la cotation la plus récente dont la date est ≤ celle de la transaction. C'est une méthode **indépendante** du forward-fill de `int_fx_rates_daily`, donc on ne teste pas un calcul avec lui-même.
- Le test renvoie les lignes en erreur : 0 ligne = succès. Seuil : 0,0001 = 0,01 %.

---

## 8. Sprint 5 — CI GitHub Actions (US 5.1)

### 8.1 Schéma isolé par PR
**Fichier : `dbt/macros/generate_schema_name.sql`**
```sql
{%- elif target.name == 'ci' -%}
    {{ default_schema }}
```
- Le profil `ci` (`profiles.yml.example`) a `schema: "CI_{{ env_var('DBT_CI_PR_ID') }}"`, donc `CI_42` pour la PR 42.
- En CI, **tous** les modèles vont dans ce schéma unique. Avant, on avait `CI_staging_42`, `CI_marts_core_42`… Avec un seul schéma, le nettoyage se fait en une commande.

### 8.2 Nettoyage
**Fichier : `dbt/macros/drop_ci_schema.sql`**
```sql
{% set schema_name = (schema_name or target.schema) | upper %}
{% if not schema_name.startswith('CI_') %}  → erreur
{% if target.database | upper == 'FINTRACK_PROD' %} → erreur
{% do run_query("drop schema if exists " ~ target.database ~ "." ~ schema_name ~ " cascade") %}
```
Deux garde-fous :
- seul un schéma `CI_*` peut être supprimé ;
- jamais dans `FINTRACK_PROD`.

Testé : `drop_ci_schema` sur `DEV_JAY` est refusé, sur `CI_0` il supprime le schéma.

### 8.3 Le workflow
**Fichier : `.github/workflows/dbt_ci.yml`** (créé : il n'existait pas)

Deux jobs :

**`slim-ci`**, sur `pull_request` :
1. Checkout, Python 3.11, `pip install dbt-core~=1.10.5 dbt-snowflake~=1.10.0` (1.10.5 minimum pour la syntaxe `arguments:` des tests).
2. Copie de `profiles.yml.example` dans `dbt/ci_profiles/`, avec `DBT_PROFILES_DIR` qui pointe dessus.
3. Téléchargement de l'artefact `prod-manifest` (dernier run sur `main`) dans `<repo>/prod-artifacts`.
4. Si le manifest existe, sélection `--select "state:modified+" --defer --state ../prod-artifacts` :
   - `state:modified+` : seulement les modèles modifiés par rapport à la prod, plus leurs descendants ;
   - `--defer` : pour les modèles non reconstruits, les `ref()` pointent vers la prod au lieu du schéma CI vide.
5. Construction :
   ```bash
   dbt run   --target ci --empty        ...   # crée les tables vides (requis par les tests unitaires)
   dbt build --target ci --full-refresh ...   # build réel + tests complets
   ```
   - Les tests unitaires ont besoin que la relation du modèle testé existe : c'est l'usage documenté par dbt.
   - `--full-refresh` est nécessaire : sans lui, les modèles incrémentaux créés vides par `--empty` liraient `max(_loaded_at)` = NULL et ne chargeraient rien.
6. Upload des `run_results.json` et `manifest.json` en artefact (7 jours).
7. `if: always()` → `dbt run-operation drop_ci_schema` : nettoyage même en cas d'échec.

**`publish-prod-manifest`**, sur `push` vers `main` :
- `dbt parse --target prod` produit `manifest.json` (aucune connexion Snowflake n'est ouverte) ;
- upload en artefact `prod-manifest` (90 jours) : c'est l'état de référence des PR suivantes.

`concurrency` annule le run précédent d'une même PR à chaque nouveau push.

**Validation en local :**
```bash
DBT_CI_PR_ID=0 dbt run --target ci --empty
DBT_CI_PR_ID=0 dbt build --target ci --full-refresh
```
Résultat : 148/148 en 1 min 03 s, puis le schéma `CI_0` est supprimé.

---

## 9. Sprint 5 — DAG Airflow (US 5.2)

**Fichier : `orchestration/airflow_dag_fintrack.py`** (squelette réécrit)

### Structure
```
fintrack_hourly_pipeline (toutes les heures à :15)
├── ingestion
│   ├── wait_for_files     FileSensor, mode reschedule, soft_fail
│   ├── put_to_stage       snowsql PUT
│   ├── copy_into_raw      SQLExecuteQueryOperator → orchestration/sql/copy_into_raw.sql
│   └── archive_files
├── ingestion_done         EmptyOperator, trigger_rule NONE_FAILED
├── dbt
│   ├── dbt_deps
│   ├── source_freshness   dbt source freshness (échoue si une source dépasse error_after)
│   ├── dbt_snapshot
│   ├── build_staging      dbt build --select tag:staging
│   ├── build_critical     dbt build --select tag:critical (fct_transactions)
│   └── build_marts        dbt build --select tag:marts --exclude tag:critical
└── reverse_etl
    ├── trigger_hightouch_sync   HttpOperator POST
    └── wait_hightouch_sync      HttpSensor (attend status success)

fintrack_weekly_full_tests (dimanche 05:00)
└── dbt_test_full          dbt test --vars '{tests_complets: true}'
```

### Explications
- **Horaire** : le SLA gold est de 2 h. Avec un run par heure, on garde une marge d'un run raté.
- **`max_active_runs=1`** : deux MERGE simultanés sur la même table entreraient en conflit.
- **`dagrun_timeout=115 min`** : aligné sur le dernier SLA de tâche. La première version (90 min) aurait tué le run avant le reverse ETL ; corrigé.
- **`mode="reschedule"`** sur les sensors : le worker est libéré entre deux vérifications au lieu de rester bloqué.
- **`soft_fail=True`** + **`TriggerRule.NONE_FAILED`** : s'il n'y a pas de fichier cette heure-ci, l'ingestion passe en *skipped* et dbt tourne quand même (des mises à jour peuvent arriver par d'autres flux). Un vrai *échec* d'ingestion bloque dbt.
- **`dbt build`** plutôt que `run` puis `test` : les tests d'un modèle bloquent son aval dans le même run. C'est aussi ce qui contourne la particularité des tests unitaires sous dbt Fusion.
- **Retries différenciés** (dictionnaires `RETRY_*` dépliés dans chaque tâche avec `**`) :
  - ingestion : 3 retries, backoff exponentiel (réseau, fichiers en retard) ;
  - dbt : 1 retry à 5 min (une erreur dbt est rarement transitoire) ;
  - reverse ETL : 5 retries (API externe, erreurs 429 / 5xx).
- **SLA par tâche** (`sla=timedelta(...)`) et **`sla_miss_callback`** → Slack. Ce paramètre existe en Airflow 2, il a été supprimé en Airflow 3.
- **Slack** : `on_failure_callback=notify_failure` dans `default_args`, donc appliqué à toutes les tâches. La fonction `_slack()` attrape les exceptions : une alerte qui échoue ne doit pas masquer l'erreur d'origine.

### `orchestration/sql/copy_into_raw.sql` (nouveau)
- Les COPY horaires utilisent `PATTERN` (tous les fichiers du stage qui correspondent au motif).
- `COPY` ne recharge jamais un fichier déjà chargé : Snowflake garde l'historique des chargements 64 jours. La tâche est donc rejouable sans doublon.
- La dernière requête fait **échouer la tâche** si des lignes ont été rejetées :
  ```sql
  SELECT 1 / IFF(SUM(error_count) > 0, 0, 1) AS assert_zero_erreur ...
  ```
  Une division par zéro provoque une erreur SQL, donc un échec de la tâche, donc un retry et une alerte Slack.

**Validation** : Airflow 2.10.5 installé dans un venv jetable, puis `DagBag(dag_folder=...)` → `import_errors: {}`, les 2 DAG chargés, 13 + 1 tâches, retries et SLA vérifiés.

---

## 10. Sprint 6 — Cohortes et anomalies (US 6.1)

### `dbt/models/marts/analytics/mart_cohortes_retention.sql`
Le squelette d'origine renvoyait 100 % de rétention en dur.

```sql
comptes  : compte_id, tenant_id, cohort_mois = mois d'ouverture
activite : (compte_id, mois_transaction) distincts, transactions validées
offsets  : 0..12 via dbt_utils.generate_series(13)
grille   : chaque cohorte × chaque offset, tant que cohort_mois + offset <= dernier mois observé
actifs   : nombre de comptes distincts actifs par (cohorte, offset = datediff('month', cohorte, mois d'activité))
final    : grille LEFT JOIN actifs → coalesce(actifs, 0), taux = 100 * actifs / taille de la cohorte
```
- **La grille** fait apparaître les mois à 0 actif. Un simple `group by` les ferait disparaître.
- **La borne sur le dernier mois observé** : un mois qui n'est pas encore passé n'est pas « 0 % de rétention ».
- **Activité avant l'ouverture ignorée** (`mois_transaction >= cohort_mois`) : le générateur produit des données incohérentes de ce type.
- Ajout de `tenant_id` : en marque blanche, chaque banque veut ses propres cohortes.

### `dbt/models/marts/analytics/mart_transactions_anomalies.sql`
```sql
{%- set fenetre_90j -%}
    over (partition by compte_id order by date_transaction
          range between interval '90 days' preceding and current row)
{%- endset %}
select tx.*,
    count(*)                       {{ fenetre_90j }} as n_avec_courante,
    sum(montant_eur)               {{ fenetre_90j }} as somme_avec_courante,
    sum(montant_eur * montant_eur) {{ fenetre_90j }} as somme_carres_avec_courante
```
- **`RANGE ... INTERVAL '90 days'`** (fenêtre en temps) plutôt que `ROWS 90 PRECEDING` (fenêtre en lignes) : pour un compte peu actif, 90 lignes peuvent couvrir plusieurs années.
- **Snowflake ne supporte pas `WINDOW w AS (...)`** (erreur rencontrée). La fenêtre est donc définie une fois dans une variable Jinja (`{% set %}`) et recopiée trois fois.
- **On exclut la transaction courante de sa propre référence**, en la retirant des sommes :
  ```
  n'     = n − 1
  moyenne' = (S − x) / n'
  variance' = (Q − x² − n' · moyenne'²) / (n' − 1)     -- variance d'échantillon
  z = (x − moyenne') / √variance'
  ```
  Sans ça, un très gros montant gonfle lui-même l'écart-type et se « cache ».
- `greatest(..., 0)` protège contre une variance légèrement négative due aux arrondis.
- Minimum 10 transactions d'historique ; sinon `z_score` est NULL.
- `is_anomalie = |z| > 3` ; `severite` : low > 3, medium > 4, high > 5.

Tests associés dans **`dbt/models/marts/analytics/_analytics.yml`** (nouveau) :
- clé unique (tenant, cohorte, offset) ;
- taux entre 0 et 100 ;
- actifs ≤ taille de la cohorte ;
- `severite` dans les valeurs acceptées.

---

## 11. Sprint 6 — Contrats et versions (US 6.2)

### Le contrat
**Fichier : `dbt/models/marts/core/_core.yml`**, bloc `fct_transactions`
```yaml
- name: fct_transactions
  latest_version: 1
  config:
    access: public
    group: core
    contract:
      enforced: true
  columns:
    - name: transaction_id
      data_type: number(18,0)
      constraints:
        - type: not_null
        - type: primary_key
    ... (43 colonnes, chacune avec data_type)
```
- `contract.enforced: true` : avant de construire, dbt compare les colonnes et les types produits par le SQL à cette liste. Toute différence (colonne en plus, en moins, type différent) fait échouer le modèle. Un dashboard ne peut plus casser en silence.
- Les types sont repris du `information_schema` de la table existante. Les identifiants sont en `number(38,0)` : sans précision, dbt prévient d'un risque d'arrondi.
- Contraintes :
  - `not_null` est **appliquée** par Snowflake (la colonne est créée `NOT NULL`) ;
  - `primary_key` est **informative** (Snowflake ne l'applique pas).
- `warn_unsupported: false` évite un avertissement sur la v2, qui est une vue et ne supporte pas les contraintes.
- `access` et `group` sont placés sous `config:` (exigence de dbt Fusion).

### Les versions
```yaml
  versions:
    - v: 1
      defined_in: fct_transactions        # le fichier SQL existant
      config:
        alias: fct_transactions           # garde le nom de table historique
        latest_version_pointer:
          enabled: false                  # spécifique dbt Fusion
    - v: 2
      columns:
        - include: all                    # toutes les colonnes de la v1…
        - name: montant_hors_taxes_eur    # … + les nouvelles
          data_type: number(18,2)
        - name: taux_tva_applique
          data_type: number(5,4)
```
- `alias: fct_transactions` : sans lui, dbt renommerait la v1 en `fct_transactions_v1`. Cela casserait Metabase et obligerait à reconstruire la table de 10M lignes.
- `latest_version_pointer` : dbt Fusion crée normalement une vue `fct_transactions` qui pointe vers la dernière version. Elle entre en conflit avec l'alias (erreur rencontrée), donc on la désactive.
- `latest_version: 1` : `ref('fct_transactions')` continue de pointer sur la v1. La v2 est en pré-release, les consommateurs l'adoptent avec `ref('fct_transactions', v=2)`.

### La v2
**Fichier : `dbt/models/marts/core/fct_transactions_v2.sql`** (nouveau)
```sql
{{ config(materialized='view') }}
with v1 as (select * from {{ ref('fct_transactions', v=1) }}),
tva as (
    select *,
        case when marchand_nom is null then null
             when marchand_pays = 'FR' then 0.2000
             when marchand_pays = 'DE' then 0.1900
             ...
             else 0.2000
        end::number(5, 4) as taux_tva_applique
    from v1
)
select *, round(montant_eur / (1 + taux_tva_applique), 2)::number(18, 2) as montant_hors_taxes_eur
from tva
```
- C'est une **vue** sur la v1 : pas de second stockage des 50-100M lignes, ni de second MERGE.
- Hypothèse : taux de TVA standard par pays du marchand, NULL quand il n'y a pas de marchand (virements…). Elle est à valider par la finance (voir `docs/CONTRACTS.md`).

### Exposures
**Fichier : `dbt/models/marts/_exposures.yml`** (nouveau). Il déclare les consommateurs : dashboard tenants, dashboard rapprochement, sync CRM Hightouch.
- Ils apparaissent dans le lineage (`dbt docs`).
- En CI, `state:modified+` montre quels usages une PR impacte.
- C'est l'étape 3 de la stratégie de migration (« notifier via exposures »).

---

## 12. Sprint 6 — Elementary (US 6.3)

**Fichier : `dbt/packages.yml`** — package décommenté :
```yaml
- package: elementary-data/elementary
  version: [">=0.20.0", "<1.0.0"]     # 0.26.1 installé, compatible dbt Fusion
```

**Fichier : `dbt/dbt_project.yml`**
```yaml
models:
  elementary:
    +schema: elementary          # → DEV_JAY_elementary en dev
```
Les 30 modèles d'Elementary stockent les résultats des tests, des runs et des métriques. Ils sont alimentés par leurs hooks `on-run-start` / `on-run-end`.

**Fichier : `dbt/models/marts/core/_core.yml`** — test de volumétrie sur `fct_transactions` :
```yaml
data_tests:
  - elementary.volume_anomalies:
      arguments:
        timestamp_column: date_transaction
        time_bucket: {period: day, count: 1}          # un point par jour
        training_period: {period: day, count: 60}     # apprentissage sur 60 jours
        detection_period: {period: day, count: 7}     # détection sur les 7 derniers
        detection_delay:
          period: day
          count: "{{ var('elementary_detection_delay_days', 0) | int }}"
        anomaly_direction: both                       # pics et creux
        anomaly_sensitivity: 3                        # seuil à 3 écarts-types
      config:
        severity: warn
```
- Elementary compte les lignes par jour et calcule moyenne et écart-type sur la période d'apprentissage. Un jour hors de `moyenne ± 3σ` est signalé comme anomalie.
- `detection_delay` : les données synthétiques datent de 2023-2024, alors qu'Elementary raisonne par rapport à aujourd'hui. Avec `--vars '{elementary_detection_delay_days: 655}'`, la fenêtre recule jusqu'à la période des données. En prod, ce délai vaut 0.

**Résultat** : 2 anomalies détectées.
- le 29/12/2024, 38 883 lignes contre ≈ 13 000 attendues (fichier rejoué simulé, section 14) ;
- le 01/01/2025, 0 ligne (trou réel de la simulation).

**Rapport HTML : `docs/elementary/elementary_report.html`**, généré par la CLI `edr report`. La CLI a été installée dans un venv jetable, avec un profil `elementary` temporaire du scratchpad.

---

## 13. Fichiers de configuration du dépôt

| Fichier | Rôle |
|---|---|
| `.gitignore` (racine) | exclut `.venv/`, `data/` (≈ 3 Go de CSV), `logs/`, `__pycache__`, `prod-artifacts/` |
| `dbt/.gitignore` | exclut `target/`, `dbt_packages/`, `logs/`, `ci_profiles/`, `profiles.yml` (secrets) |
| `.sqlfluff` | linter SQL, dialecte Snowflake, templater **jinja** (pas besoin de connexion Snowflake) ; annoncé par le README mais absent |
| `.pre-commit-config.yaml` | hooks Git : espaces, fin de fichier, YAML valide, gros fichiers, sqlfluff ; requis par `pre-commit install` dans le README |
| `README.md` | mis à jour : versions dbt, étape « 5 bis » du clone RAW dev, nouvelles commandes (`drop_ci_schema`, `tests_complets`, microbatch, `edr`), arborescence |
| `dbt/dbt_project.yml` | `test-paths`, hooks `{% do %}`, `require-dbt-version >=1.9,<3`, snapshots en `+schema`, `store_failures` prod, Elementary, vars commentées |

Commentaires `TODO` obsolètes remplacés par la décision prise :
- `dbt/models/marts/core/bridge_comptes_titulaires.sql` ;
- `dbt/models/marts/finance/mart_solde_journalier.sql` ;
- `dbt/models/intermediate/int_transactions_normalisees.sql`.

Les tags `todo` ont été retirés des deux marts du Sprint 6.

---

## 14. Simulations exécutées dans Snowflake (hors dépôt)

Toutes ont été faites **uniquement dans le clone `FINTRACK_DEV.DEV_JAY_RAW`**. Les scripts sont restés dans le scratchpad de session et ne sont pas versionnés. Le clone ne reflète donc plus la prod.

| # | Simulation | SQL principal | But |
|---|---|---|---|
| 1 | Backfill `_loaded_at` | `update raw_* set _loaded_at = '<heure du COPY>' where _loaded_at is null` | réparer l'incrémental |
| 2 | Historique au fil de l'eau | `update raw_transactions set _loaded_at = loaded_at` | un `_loaded_at` réaliste, étalé sur 2 ans |
| 3 | Delta J+1 | table `sim_changements` (1 250 transactions), `update` des statuts, `insert` de 13 859 nouvelles lignes (copie du 24/12 décalée de 7 j, IDs + 100M) | tester le merge |
| 4 | Delta J+2 | idem (`sim_changements_j2`, IDs + 200M) | mesurer le run final |
| 5 | 3 changements de comptes | `update raw_comptes` sur les comptes 7, 8 et 10, puis compte 7 réactivé | SCD2 |
| 6 | Fichier rejoué | `insert` ×2 de la journée du 29/12/2024 (IDs + 300M / 600M) | anomalie Elementary |

Autres actions faites dans Snowflake (dev) :
- **Search Optimization** activée à la main sur `DEV_JAY_MARTS_CORE.FCT_TRANSACTIONS`, pour mesurer (celle sur `compte_id` a été retirée ensuite) ;
- **schéma `CI_0`** créé par le test de CI locale, puis supprimé ;
- **une requête Elementary trop lourde annulée** (`SYSTEM$CANCEL_QUERY`) pour ne pas consommer de crédits.

Mesures :
- `ACCOUNT_USAGE.QUERY_HISTORY` / `INFORMATION_SCHEMA.QUERY_HISTORY`, agrégés par `model=` ;
- `SYSTEM$CLUSTERING_INFORMATION`, `SYSTEM$ESTIMATE_SEARCH_OPTIMIZATION_COSTS`, `GET_QUERY_OPERATOR_STATS`, `WAREHOUSE_METERING_HISTORY`.

---

## 15. Git : branche, commits, push

Dépôt : `fintrack-avance/` (remote `git@github.com:JayTek26/fintrack-avance.git`).

```
* c582cf7  (feat/sprint3-6-perf-scale)  Sprints 3 à 6 : incrémental, perf, SCD2, CI/CD, contrats, observabilité
* 937550f  (main)                        Initialisation du dépôt : briefing de mission
```
- `main` n'avait aucun commit. Un commit minimal (`MISSION.md` + `.gitignore`) sert de base à la PR.
- Branche nommée selon la convention du README (`feat/<sprint>-<slug>`) ; elle contient 72 fichiers (5 Mo, dont 4,8 Mo de rapport Elementary).
- Vérifié avant le commit : aucun mot de passe ni identifiant de compte Snowflake dans les fichiers.
- Auteur : `jay <cohortedata@gotolearn.com>`, passé avec `git -c` (aucune identité Git n'était configurée ; ta config n'a pas été modifiée).
- Les deux branches sont poussées. La PR reste à ouvrir depuis https://github.com/JayTek26/fintrack-avance/pull/new/feat/sprint3-6-perf-scale (`gh` n'est pas installé). La description est prête dans le scratchpad de session (`pr_description.md`).
- **Ce fichier `explain.md` n'est pas encore commité.**

---

## 16. Ce qui n'a pas été fait et pourquoi

| Point | Raison | Que faire |
|---|---|---|
| Backfill `_loaded_at` en **prod** | écriture prod refusée par le mode auto | lancer les UPDATE du PERF_LOG avec FINTRACK_TRANSFORM_ROLE |
| Premier déploiement prod | même raison ; la CI `--defer` en dépend | `dbt build --target prod --full-refresh` sur un warehouse LARGE |
| CI verte sur la PR | secrets GitHub absents, prod non déployée | ajouter les 4 secrets (en-tête de `dbt_ci.yml`) |
| Réglages warehouses | le rôle dev n'a pas les droits `ALTER WAREHOUSE` | commandes dans PERF_LOG, section Warehouses |
| Profondeur de clustering < 3 | 3,09 au mieux : 288 valeurs de clé pour 32 partitions à l'échelle L | revérifier à l'échelle prod |
| Captures Query Profile | impossible depuis le terminal | query_id dans PERF_LOG → Snowsight |
| Retrait des clés de clustering de stg / raw | recommandé, non testé sur un cycle complet | à valider |
| Test unitaire `fct_virements` en `dbt test` seul | particularité de dbt Fusion 2.0.8 (passe en `dbt build`) | utiliser `dbt build` |

---

## 17. Index : fichier par fichier

**Créés**

| Fichier | Section |
|---|---|
| `.github/workflows/dbt_ci.yml` | 8 |
| `.gitignore`, `dbt/.gitignore`, `.sqlfluff`, `.pre-commit-config.yaml` | 13 |
| `dbt/macros/purge_transactions_hors_perimetre.sql` | 4 |
| `dbt/macros/get_where_subquery.sql` | 6.2 |
| `dbt/macros/ensure_search_optimization.sql` | 6.4 |
| `dbt/macros/drop_ci_schema.sql` | 8.2 |
| `dbt/models/marts/core/fct_transactions_v2.sql` | 11 |
| `dbt/models/marts/analytics/_analytics.yml` | 10 |
| `dbt/models/marts/_exposures.yml` | 11 |
| `dbt/tests/assert_fct_montant_eur_taux_du_jour.sql` | 7.4 |
| `dbt/tests/assert_scd2_une_version_courante.sql`, `assert_scd2_periodes_contigues.sql` | 7.3 |
| `orchestration/sql/copy_into_raw.sql` | 9 |
| `docs/RAPPORT_FINAL.md`, `docs/elementary/elementary_report.html` | 12 |
| `explain.md` | ce document |

**Modifiés**

| Fichier | Changement | Section |
|---|---|---|
| `scripts/snowflake/03_stage_and_copy.sql` | COPY par nom + `INCLUDE_METADATA`, COPY titulaires | 1 |
| `dbt/dbt_project.yml` | paths, hooks, version, snapshots, tests, Elementary, vars | 3, 4, 6, 7, 12 |
| `dbt/packages.yml`, `package-lock.yml` | Elementary | 12 |
| `dbt/models/staging/_sources.yml` | clone dev, tests fenêtrés | 1, 6.2 |
| `dbt/models/staging/_staging.yml` | tests fenêtrés | 6.2 |
| `dbt/models/staging/stg_transactions.sql` | merge_update_columns, event_time, limite CI | 4, 5, 8 |
| `dbt/models/intermediate/int_fx_rates_daily.sql` | grille jusqu'à aujourd'hui, `date_cotation_source` | 2 |
| `dbt/models/intermediate/int_transactions_normalisees.sql` | commentaire | 13 |
| `dbt/models/marts/core/fct_transactions.sql` | FX, lookback, merge cols, post-hooks | 2, 4, 6 |
| `dbt/models/marts/core/fct_transactions_microbatch.sql` | prototype (était vide) | 5 |
| `dbt/models/marts/core/fct_virements.sql` | montants EUR | 2 |
| `dbt/models/marts/core/dim_comptes_scd2.sql` | colonnes, doc | 7.2 |
| `dbt/models/marts/core/bridge_comptes_titulaires.sql` | commentaire | 13 |
| `dbt/models/marts/core/_core.yml` | contrat, versions, SCD2, Elementary, tests fenêtrés | 6, 7, 11, 12 |
| `dbt/models/marts/finance/mart_solde_journalier.sql` | virements en EUR, commentaire | 2 |
| `dbt/models/marts/analytics/mart_cohortes_retention.sql` | implémentation | 10 |
| `dbt/models/marts/analytics/mart_transactions_anomalies.sql` | implémentation | 10 |
| `dbt/snapshots/snapshot_comptes.sql` | `schema`, `hard_deletes` | 7.1 |
| `dbt/macros/set_query_tag.sql` | tag par modèle | 6.1 |
| `dbt/macros/generate_schema_name.sql` | schéma CI unique | 8.1 |
| `dbt/unit_tests/unit_fct_virements.yml`, `unit_stg_transactions_incremental.yml` | réécrits | 3 |
| `orchestration/airflow_dag_fintrack.py` | DAG complet | 9 |
| `README.md` | setup, commandes, arborescence | 13 |
| `docs/PERF_LOG.md`, `docs/DESIGN.md`, `docs/CONTRACTS.md` | remplis | — |

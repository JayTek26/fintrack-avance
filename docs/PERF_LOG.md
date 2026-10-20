# Performance Log — FinTrack Perf & Scale

Journal de performance avant / après optimisations. Toutes les mesures du Sprint 3 ont été faites le 20/10/2026 sur le dataset scale L (10M transactions, 200k comptes), en target `dev` (WH_DEV, XSMALL, 4 threads, dbt Fusion 2.0.8).

## Chargement initial

| Scale | Taille CSV (Go) | Durée génération | Durée PUT | Durée COPY | Erreurs |
|-------|-----------------|------------------|-----------|------------|---------|
| M     | non réalisé     |                  |           |            |         |
| L     | 3,18 (compressé `.csv.gz`, dont 3,15 pour les transactions) | à compléter | 2 h 15 min 19 s (8 118,8 s) | 2 min 44 s (164,1 s, dont 151,4 s pour les transactions) | 0 |
| XL    | non réalisé     |                  |           |            |         |

Notes sur le scale L (10 000 000 transactions, 200 000 comptes, 20 000 virements) :
- PUT du 13/10/2026 depuis un MacBook Air avec SnowSQL 1.5.1 : 3 177 605 680 octets en 8 118,8 s, soit ≈ 0,37 MiB/s (≈ 3,1 Mbit/s). Le temps dépend de la bande passante montante du poste client, pas de Snowflake.
- COPY du 14/10/2026 sur WH_INGESTION en LARGE (8 crédits/h), un fichier par table : transactions 151,4 s (≈ 66 000 lignes/s, ≈ 0,34 crédit), comptes 6,9 s, virements 1,4 s, fx_rates 1,2 s, categories 1,7 s, tenants 1,5 s. Total : 10 226 621 lignes chargées.
- Erreurs : `error_count = 0` sur raw_transactions, raw_comptes, raw_virements, raw_fx_rates et raw_categories (`ON_ERROR = 'CONTINUE'`). raw_tenants non vérifié.
- Clustering de raw_transactions posé après le chargement (un `COPY INTO` échoue si la table cible a une clé de clustering contenant `DATE_TRUNC`). Mesure du 19/10/2026 : profondeur moyenne 6,859, chevauchements moyens 11,0, 78 partitions. Ligne de départ pour le Sprint 3.
- Hypothèse non testée : un fichier gzip unique n'exploite qu'une petite partie des 8 nœuds d'un LARGE, donc un XSMALL ou un SMALL suffirait probablement pour ce chargement. WH_INGESTION est depuis repassé en XSMALL.

### Incident découvert au Sprint 3 : `_loaded_at` NULL

Constat du 20/10/2026 : `_loaded_at` est NULL sur 100 % des lignes de raw_transactions, raw_comptes, raw_virements, raw_tenants, raw_categories et raw_fx_rates (raw_titulaires et raw_compte_titulaires, chargées le 19/10, ne sont pas concernées).

- **Cause :** `COPY INTO` positionnel (`SKIP_HEADER = 1`) avec `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE`. Le CSV a une colonne de moins que la table, donc Snowflake remplit la dernière colonne avec NULL : le `DEFAULT CURRENT_TIMESTAMP()` ne s'applique pas.
- **Impact :** tous les filtres incrémentaux (`_loaded_at >= max(_loaded_at) - N`) renvoyaient 0 ligne. Les runs incrémentaux du 20/10 au matin réussissaient sans rien traiter. La fraîcheur des sources ne pouvait pas non plus être mesurée.
- **Correctif pour les prochains chargements :** `scripts/snowflake/03_stage_and_copy.sql` passe en `PARSE_HEADER = TRUE` + `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` + `INCLUDE_METADATA = (_loaded_at = METADATA$START_SCAN_TIME)`, validé sur un fichier réel en dev. Le même pattern est utilisé dans `orchestration/sql/copy_into_raw.sql`.
- **Données existantes :** en prod, il reste **à corriger**. Le backfill n'a pas été exécuté sur `FINTRACK_PROD.RAW`. En dev, il a été fait sur un clone zero-copy (`FINTRACK_DEV.DEV_JAY_RAW`, voir ci-dessous) avec l'heure réelle du COPY relevée dans `ACCOUNT_USAGE.COPY_HISTORY` (14/10/2026 08:55, heure du compte). Requête type pour la prod, par table : `UPDATE raw_transactions SET _loaded_at = '2026-10-14 08:55:19' WHERE _loaded_at IS NULL;` (≈ 27 s sur XSMALL pour 10M lignes en dev).

## Protocole de mesure

- **Environnement isolé :** en target `dev`, les sources lisent `FINTRACK_DEV.DEV_<USER>_RAW`, un clone zero-copy de `FINTRACK_PROD.RAW` (`_sources.yml`, vars `raw_database` / `raw_schema`). On peut y simuler des mises à jour sans toucher à la prod.
- **Historique réaliste :** dans le clone, `_loaded_at = loaded_at` (horodatage d'ingestion du core banking, 2023-01-01 → 2024-12-31), comme si l'historique avait été chargé au fil de l'eau et non en un seul COPY.
- **Deltas simulés :**
  - J+1 (01/01/2025) : 13 859 nouvelles transactions, 1 000 passages `en_attente → validee`, 200 `en_attente → rejetee`, 50 mises à jour tardives sur des transactions de 2023.
  - J+2 (02/01/2025) : 13 766 nouvelles transactions et les mêmes types de changements.
- **Sources des chiffres :** `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` (lignes insérées / mises à jour, partitions, spill) et `INFORMATION_SCHEMA.QUERY_HISTORY` (temps compilation / exécution), agrégés par modèle grâce au query tag `...|model=<nom>` (macro `set_query_tag`, enrichie au Sprint 3). « Elapsed » = temps passé côté Snowflake ; « Wall » = durée affichée par dbt.

## Baseline (avant optimisations)

Warehouse : WH_DEV (XSMALL)  Target : dev  Date : 20/10/2026, 16:40 UTC
Run : `dbt build --full-refresh` (ce que faisait de fait le pipeline hérité : aucun incrémental ne fonctionnait).

| Modèle | Rows | Bytes scannés | Elapsed (s) | Partitions scanned | Credits (≈) |
|--------|------|---------------|-------------|--------------------|-------------|
| stg_transactions | 10 000 000 | 2,68 Go (+ **11,9 Go de spill local**) | 58,9 | 92 / 92 | 0,016 |
| fct_transactions | 9 452 515 | 0,90 Go | 21,1 | 140 / 215 | 0,006 |
| mart_solde_journalier | 9 164 486 | 0,06 Go | 5,5 | 34 / 34 | 0,002 |
| mart_tenant_kpis_daily | 8 760 | 0,12 Go | 4,2 | 33 / 33 | 0,001 |
| Tests + métadonnées | — | 16,6 Go (*) | 209,5 | — | 0,058 |
| **Full run** | 40 016 788 | 4,43 Go (*) | **347,5** | 993 / 1 132 | **≈ 0,058** (wall 3 min 27 s) |

(*) `INFORMATION_SCHEMA` et `ACCOUNT_USAGE` ne comptent pas les octets de la même manière : 28,8 Go d'un côté, 4,43 Go de l'autre pour le même run. Les comparaisons se font toujours à source identique.
Crédits ≈ wall-clock × 1 crédit/h (XSMALL) : le warehouse reste actif pendant tout le run.

## Après optimisations Sprint 3

Warehouse : WH_DEV (XSMALL)  Target : dev  Date : 20/10/2026, 17:26 UTC
Run : `dbt build` incrémental sur le delta J+2, avec toutes les optimisations.

| Modèle | Rows (ins / upd / del) | Bytes scannés | Elapsed (s) | Partitions scanned | Credits (≈) | Gain |
|--------|------------------------|---------------|-------------|--------------------|-------------|------|
| stg_transactions | 13 766 / 16 299 / 0 | 0,92 Go, spill 0 | 12,7 | 115 / 207 | 0,004 | **−78 %** |
| fct_transactions | 13 002 / 15 110 / 200 | 0,86 Go | 17,1 | 179 / 394 | 0,005 | −19 % |
| Tests + métadonnées | — | 2,6 Go (*) | 64,4 | — | — | **−69 %** |
| **Full run** | 20 721 179 | 2,46 Go (*) | **155,9** | 953 / 1 855 | **≈ 0,033** (wall 2 min 00 s) | **−55 % elapsed, −42 % wall** |

Étapes intermédiaires, pour attribuer les gains :

| Run | Wall | Elapsed Snowflake | Commentaire |
|-----|------|-------------------|-------------|
| Baseline full-refresh | 3 min 27 s | 347,5 s | |
| Incrémental J+1, lookback 7 j, tests complets | 2 min 46 s | 274,4 s | stg −73 %, mais fct ne réécrit pas moins : 91 480 lignes mises à jour |
| `dbt test` seul, avant / après fenêtrage + `store_failures` limité à la prod | 2 min 54 s → **58 s** | 209,5 s → 54,1 s | 16,6 Go → 5,3 Go scannés ; 774 → 479 requêtes |
| Incrémental J+2, lookback 1 j, tests fenêtrés | 2 min 00 s | 155,9 s | fct : 15 110 lignes mises à jour au lieu de 91 480 (−83 %) |

Ce qui a été changé, par ordre de gain :
1. **Incrémental fonctionnel** (`_loaded_at` réparé). Le full-refresh de stg_transactions déverse 11,9 Go sur disque local sur un XSMALL : c'est le `ROW_NUMBER()` de déduplication sur 10M lignes. En incrémental, ce spill tombe à 0.
2. **Tests fenêtrés.** Sur un run incrémental, les tests coûtaient **66 % du temps warehouse** (180 s sur 274 s) : chaque `unique` / `not_null` / `relationships` scannait les 10M lignes. Une surcharge de `get_where_subquery` restreint les tests des grosses tables à la fenêtre incrémentale (placeholder `__fenetre_incrementale__`). Les tests complets passent dans le DAG hebdomadaire et en CI (`--vars '{tests_complets: true}'`).
3. **`store_failures` uniquement en prod.** Il transformait chaque test en `CREATE TABLE AS` + `SELECT`.
4. **Lookback 7 j → 1 j.** `_loaded_at` est un horodatage d'ingestion monotone, et une transaction mise à jour est rechargée avec un nouveau `_loaded_at`. Une fenêtre de 7 jours réécrivait donc environ 97k lignes inchangées à chaque run.
5. **Purge des transactions sorties du périmètre** (post-hook `purge_transactions_hors_perimetre`). C'est une correction fonctionnelle, pas un gain de performance : sans elle, les 200 transactions `en_attente → rejetee` restaient en `en_attente` dans fct_transactions.

**Ce qui n'a pas bougé :** fct_transactions (−19 %). Le `MERGE` doit joindre la cible entière sur `transaction_id` : 0,86 Go, 179 partitions sur 394. Un `incremental_predicates` sur `date_transaction` réduirait ce scan, mais il ferait insérer en doublon les mises à jour tardives (les 50 transactions de 2023), donc il est écarté.

**Overhead dbt :** environ la moitié du wall-clock n'est pas du SQL. `dim_tenants` (12 lignes) affiche 23 s côté dbt pour 1,5 s côté Snowflake : compilation, introspection, allers-retours. Sur 117 tests, la compilation Snowflake représente à elle seule 41,7 s des 54,1 s.

### Extrapolation à la prod

On ne peut pas mesurer la prod (le pipeline dbt n'y a jamais été déployé). Le coût d'un run incrémental dépend du **delta** et non de l'historique, à deux exceptions près, qui sont linéaires en volume : le scan de la cible par le `MERGE` (0,86 Go pour 10M lignes) et les marts en `table` reconstruits à chaque run (mart_solde_journalier, mart_transactions_anomalies : environ 9M lignes chacun).
À 100M transactions/mois, le delta horaire est d'environ 140k lignes, et les marts complets deviennent le poste dominant. Prochaine étape : passer mart_solde_journalier et mart_transactions_anomalies en incrémental par partition de jours.

## Merge vs Microbatch (US 3.2)

Prototype : `fct_transactions_microbatch` (`batch_size='day'`, `lookback=3`, `event_time='date_transaction'`, `concurrent_batches=true`), désactivé par défaut (`--vars '{enable_microbatch: true}'`).

| Run | Wall | Elapsed Snowflake | Requêtes | Bytes (*) |
|-----|------|-------------------|----------|-----------|
| Microbatch, chargement initial décembre 2024 (32 batchs) | 1 min 17 s | 110,3 s | 283 | 29,7 Go |
| Microbatch, run quotidien (4 batchs = J + lookback 3) | 27,6 s | **10,4 s** | 36 | 3,27 Go |
| Merge fct_transactions, run quotidien (J+2) | — | 17,1 s | 27 | 1,81 Go |

- Microbatch est **plus rapide sur le modèle seul** (10,4 s contre 17,1 s) : il remplace 4 jours entiers sans joindre la table cible.
- **Il ne capte pas les mises à jour tardives** : il ne retraite que les fenêtres de `date_transaction` comprises dans `[aujourd'hui − lookback, aujourd'hui]`. Les 50 changements de statut sur des transactions de 2023 resteraient faux. Or un changement de statut AML ou de rapprochement peut arriver des semaines après la transaction.
- **Il casse le clustering :** chaque batch écrit des partitions qui mélangent tous les tenants. Profondeur 36 sur fct_transactions_microbatch, contre 3,09 pour le merge après rebuild.
- Le backfill initial est cher : 283 requêtes et 110 s pour un seul mois, soit environ 25 min estimées pour l'historique de 2 ans sur XSMALL. Deux ans représentent par ailleurs environ 730 batchs, contre une seule requête pour le merge.

**Recommandation : merge.** La clé fonctionnelle du pipeline, ce sont des mises à jour d'état (statut, AML, rapprochement) sur des transactions de n'importe quel âge. Microbatch ne les capte que si l'on élargit le lookback à la plus vieille mise à jour possible, ce qui annule son gain. Microbatch resterait pertinent pour des faits immuables (logs d'événements, cotations FX).

## Clustering (US 3.3)

| Table | Clé | Partitions | Profondeur moy. | Chevauchements | Contexte |
|-------|-----|-----------:|----------------:|---------------:|----------|
| fct_transactions | (tenant_id, mois) | 32 | **3,09** | 2,94 | juste après un rebuild (CTAS trié par la clé) |
| fct_transactions | (tenant_id, mois) | 33 | 3,18 | 3,15 | après le merge J+1 |
| fct_transactions | (tenant_id, mois) | 42 | 11,95 | 17,52 | après J+2 + fichier rejoué de 27k lignes, avant reclustering |
| stg_transactions | (tenant_id, mois) | 113 | 10,5 | 17,06 | après les merges |
| fct_transactions_microbatch | (tenant_id, mois) | 42 | 36,0 | 40,0 | écrit batch par batch |
| raw_transactions (prod) | (tenant_id, mois) | 78 | 6,86 | 11,0 | mesure du 19/10 |

Lecture :
- **Cible < 3 : non tenue à l'échelle L (3,09 au mieux).** Il y a 288 valeurs de clé (12 tenants × 24 mois) pour 32 partitions : chaque partition contient forcément plusieurs valeurs, d'où une profondeur plancher d'environ 3. À 100M lignes (environ 330 partitions), chaque couple (tenant, mois) occupe plusieurs partitions entières (*constant partitions*) et la profondeur après reclustering descend mécaniquement sous 3. À revérifier en prod.
- **Les merges dégradent la profondeur entre deux reclusterings** (3,09 → 11,95). L'Automatic Clustering est actif. Il a consommé 0,025 crédit sur fct_transactions et 0,076 sur stg_transactions en dev le 20/10, soit 0,139 crédit sur le compte.
- **Le pruning est bon là où il compte :** la requête dashboard filtrée par tenant et trimestre lit 4 partitions sur 33 (88 % de pruning).
- **Décision fct_transactions :** clé `(tenant_id, date_trunc('month', date_transaction))` conservée. Tous les accès BI filtrent par tenant (isolation multi-tenant) puis par période.
- **Recommandation stg_transactions :** retirer la clé (tenant, mois). Son seul consommateur est le filtre incrémental sur `_loaded_at` et le `MERGE` sur `transaction_id`, et c'est la plus grosse dépense d'auto-clustering. L'ordre d'insertion naturel suit déjà `_loaded_at`. Non appliqué : à valider sur un cycle complet.
- **Recommandation RAW :** rester en **append-only**. Les 1 250 mises à jour *en place* simulées dans le clone ont fait passer sa profondeur à 33 et anéanti le pruning sur `_loaded_at` : des partitions de 2023 ont reçu un `_loaded_at` de 2025. Une mise à jour de statut doit arriver comme une nouvelle ligne, dédupliquée en staging (`QUALIFY ROW_NUMBER()`, déjà en place).

## Analyse Query Profile

Trois requêtes fréquentes, exécutées sur dev avec `USE_CACHED_RESULT = FALSE`, profils extraits avec `GET_QUERY_OPERATOR_STATS` (identifiants ci-dessous, rejouables dans Snowsight → Query History → Profile). Captures d'écran Snowsight : à ajouter (non disponibles depuis la CLI).

### Query 1 : lookup support / rapprochement par `external_transaction_id`

`select ... from fct_transactions where external_transaction_id = :id` — query_id `01c7dc4d-030c-5e8d-0033-aad70015c40e`

- Bytes scanned : 840,8 Mo
- Partitions scanned / total : 33 / 33
- Pruning efficiency : 0 %
- Node dominant : TableScan (97,1 % du temps)
- Bytes spilled to local / remote : 0 / 0
- Durée : 686 ms (351 ms d'exécution)
- **Recommandation :** Search Optimization `EQUALITY(external_transaction_id)`. Mesuré après activation (query_id `01c7dc4f-030c-5e47-0033-aad70016d3d2`) : nœud *Search Optimization Access*, **1 / 33 partitions**, 136,3 Mo (−84 %). La latence ne bouge pas à cette échelle (952 ms, dominés par la compilation), mais le gain en octets croît linéairement avec la table. → Activée en prod par post-hook (`ensure_search_optimization`), voir Décisions.

### Query 2 : dashboard tenant — volumes mensuels sur un trimestre

`select mois_transaction, type_operation, count(*), sum(montant_eur), avg(fraud_score) from fct_transactions where tenant_id = 3 and date_transaction >= '2024-10-01' and date_transaction < '2025-01-01' group by 1, 2` — query_id `01c7dc4d-030c-5e46-0033-aad70016b41a`

- Bytes scanned : 246,3 Mo
- Partitions scanned / total : 4 / 33
- Pruning efficiency : 88 %
- Node dominant : TableScan (60 %), puis Filter et Aggregate
- Bytes spilled to local / remote : 0 / 0
- Durée : 394 ms
- **Recommandation :** rien à changer sur la table, c'est le cas d'usage pour lequel la clé de clustering est choisie. Côté BI : pointer les dashboards sur `mart_tenant_kpis_daily` (8 760 lignes) plutôt que sur le fait dès que la maille journalière suffit, et laisser le result cache jouer (WH_REPORTING `AUTO_SUSPEND = 300`).

### Query 3 : relevé d'un compte par IBAN sur 90 jours

`select ... from fct_transactions f join dim_comptes c on c.compte_id = f.compte_id where c.iban = :iban and f.date_transaction >= '2024-10-03'` — query_id `01c7dc4d-030c-5e8e-0033-aad70015f3fe`

- Bytes scanned : 591,2 Mo
- Partitions scanned / total : fct 21 / 33 ; dim_comptes 1 / 1
- Pruning efficiency : 36 % sur le fait (filtre de date seul, `tenant_id` absent du prédicat)
- Node dominant : TableScan de fct_transactions (97,9 %), avec un JoinFilter (filtre de bloom à l'exécution)
- Bytes spilled to local / remote : 0 / 0
- Durée : 1 031 ms
- **Recommandations :**
  1. Ajouter `tenant_id` au prédicat. L'application connaît toujours le tenant, et une row access policy par tenant l'imposerait de toute façon. Le pruning passe alors sur la clé de clustering.
  2. Résoudre `compte_id` d'abord (lookup sur dim_comptes), puis interroger le fait par `compte_id`. Mesuré avec la SO sur `compte_id` (`01c7dc4f-030c-5e6a-0033-aad70015645e`) : **37 ms d'exécution**, mais 18 / 33 partitions seulement. La SO sur `compte_id` est écartée (bénéfice faible, voir Décisions).
  3. La SO sur `iban` (dim_comptes) est inutile à ce volume : la table tient en 1 partition (11 Mo). À réévaluer au-delà d'environ 1M de comptes.

## Décisions prises

- **Clustering key sur fct_transactions :** `(tenant_id, date_trunc('month', date_transaction))` → Justification : filtres BI dominants (tenant puis période), 88 % de pruning sur Q2 ; profondeur 3,09 après rebuild, plancher dû au faible nombre de partitions à l'échelle L.
- **Search Optimization :** activée sur `fct_transactions.external_transaction_id` (en prod, post-hook idempotent qui survit aux `--full-refresh`) → Coût estimé : build 0,0008 crédit, stockage 86 Mo (≈ 10 % de la table) à l'échelle L, maintenance non estimable avant 7 jours d'historique (à suivre dans `METERING_HISTORY`, service `SEARCH_OPTIMIZATION`) → Bénéfice : 1 partition lue au lieu de toutes pour les lookups support / rapprochement. Écartée sur `compte_id` (21 → 18 partitions) et sur `dim_comptes.iban` (table d'une seule partition).
- **Materialized view :** non activée → Justification : les agrégats fréquents sont déjà matérialisés par dbt (`mart_tenant_kpis_daily`), et une MV sur un fait mis à jour par MERGE toutes les heures se maintiendrait en continu (coût serverless) pour un gain que le result cache couvre déjà.
- **Merge vs Microbatch :** choix **merge** → Raison : seul le merge sur `_loaded_at` capte les changements de statut sur des transactions anciennes, et il préserve le clustering. Microbatch est plus rapide de 7 s par run, mais faux sur les mises à jour tardives (voir section dédiée).
- **Lookback incrémental :** 1 jour (`incremental_lookback_days`) au lieu de 7 → 83 % de lignes réécrites en moins sur fct_transactions.
- **Tests sur grosses tables :** fenêtrés sur la fenêtre incrémentale à chaque run, complets une fois par semaine et en CI.

### Warehouses (recommandations, non appliquées : le rôle dev n'a pas les droits)

État actuel : INGESTION XSMALL (déjà réduit depuis LARGE), DEV XSMALL, ADHOC XSMALL, REPORTING SMALL multi-cluster 1-5 (auto-suspend 300 s), TRANSFORM MEDIUM multi-cluster 1-3 (auto-suspend 60 s).

```sql
-- TRANSFORM : un seul job dbt à la fois (max_active_runs=1), aucune file
-- d'attente mesurée → le multi-cluster ne sert à rien. SMALL suffit pour les
-- runs incrémentaux horaires (~12 s de MERGE en XSMALL à 10M lignes).
ALTER WAREHOUSE WH_TRANSFORM SET WAREHOUSE_SIZE = 'SMALL' MAX_CLUSTER_COUNT = 1 AUTO_SUSPEND = 60;

-- Full-refresh (rare) : warehouse plus large, le temps du rebuild, pour
-- éviter le spill (11,9 Go en XSMALL) :
--   ALTER WAREHOUSE WH_TRANSFORM SET WAREHOUSE_SIZE = 'LARGE';  -- puis retour à SMALL

-- REPORTING : garder 300 s (cache local chaud pour les dashboards) ; 15
-- tenants ne justifient pas 5 clusters, et ECONOMY évite d'en démarrer un
-- pour une pointe de quelques secondes.
ALTER WAREHOUSE WH_REPORTING SET MAX_CLUSTER_COUNT = 2 SCALING_POLICY = 'ECONOMY';

-- Plafond de sécurité par requête
ALTER WAREHOUSE WH_ADHOC SET STATEMENT_TIMEOUT_IN_SECONDS = 1800;
```

## Coûts Snowflake

| Semaine | Credits WH_INGESTION | Credits WH_TRANSFORM | Credits WH_REPORTING | Credits WH_DEV | Autres | Total |
|---------|----------------------|----------------------|----------------------|----------------|--------|-------|
| 05/10/2026 | 0,24 | 0,11 | 0,24 | — | — | 0,59 |
| 12/10/2026 | 0,82 | 0 | 0 | — | — | 0,82 |
| 19/10/2026 (au 20/10) | 0,03 | 0 | 0 | 2,58 | 2,78 (COMPUTE_WH) + 0,14 (auto-clustering) | 5,52 |
| **Total** | **1,09** | **0,11** | **0,24** | **2,58** | **2,92** | **6,93** |

Sources : `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` et `METERING_HISTORY` (relevé du 20/10/2026), semaines débutant le lundi à 00:00 (heure Pacifique, UTC-7). Le tableau des semaines précédentes ne comptait que les warehouses du projet ; la ligne du 19/10 ajoute WH_DEV et les services serverless.

Notes sur la semaine du 19/10 :
- WH_DEV (2,58) couvre l'ensemble des benchmarks du Sprint 3 : environ 10 builds complets ou incrémentaux, CI locale, microbatch, Elementary.
- COMPUTE_WH (2,78) est le warehouse par défaut du compte : il a servi aux requêtes de diagnostic lancées sans `USE WAREHOUSE` (`ACCOUNT_USAGE`, `COPY_HISTORY`), et peut-être à des sessions Snowsight. **À surveiller :** il échappe aux query tags du projet.
- Services serverless : auto-clustering 0,139, Search Optimization 0,0013, Query Acceleration 0,001.

Quota mensuel du resource monitor : 100 crédits, soit ≈ 7 % consommés.

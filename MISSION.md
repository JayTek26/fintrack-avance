# Mission — FinTrack Perf & Scale

**Client :** FinTrack Analytics — plateforme B2B2C d'analyse financière
**Rôle :** Consultant Data Engineer Senior
**Durée :** 8 à 10 jours ouvrés (6 sprints)
**Environnement :** Snowflake + dbt (Core 1.8+) + Python + GitHub Actions

---

## Contexte

Dix-huit mois ont passé depuis vos premiers travaux sur FinTrack. La fintech a pivoté : elle ne vend plus une app grand public mais licencie sa **plateforme analytique en marque blanche** à des banques partenaires européennes. Aujourd'hui, **15 tenants** (BNP, Crédit Agricole, ING, Deutsche Bank, Revolut, Qonto…) déversent leurs flux dans FinTrack.

Ordre de grandeur actuel :

- **50 à 100 millions** de transactions traitées par mois
- **~1 million** de comptes actifs
- **80 colonnes** dans la table `raw_transactions` (métadonnées core banking, KYC, AML, rapprochement, timestamps multiples)
- Multi-devises (10 devises), multi-langues, multi-fuseaux
- SLA de fraîcheur contractuelle : **2h** pour les tenants gold, **6h** pour silver, **24h** pour bronze

Vous héritez d'une pipeline dbt qui « fonctionne mais tousse » : les `dbt run` prennent 4h en prod, les warehouses coûtent 8 000 $/mois et personne ne sait pourquoi, aucune traçabilité fine des coûts, aucune stratégie incrémentale claire, les modèles reconstruisent des vues gigantesques à chaque run, et l'équipe finance n'a **aucune visibilité** sur le rapprochement bancaire.

Le CTO vous fixe trois objectifs, non-négociables :

1. **Diviser par 4 le temps d'exécution** du pipeline complet (< 1h)
2. **Diviser par 2 les coûts** de compute Snowflake sans perdre en fraîcheur
3. **Livrer une modélisation industrialisable** — contrats, versioning, tests, CI/CD

Vous disposez d'un dataset synthétique généré par le script `scripts/python/generate_data.py`, dont vous pouvez ajuster le volume via `--scale`.

---

## Contenu du repo

Consultez `README.md` pour la mise en route technique. Le briefing métier est organisé en 6 sprints ci-dessous.

---

## Sprint 1 — Provisioning & ingestion (Jour 1-2)

### User Story 1.1 — Infrastructure Snowflake
**En tant que** Data Engineer,
**je veux** provisionner l'infrastructure Snowflake selon les bonnes pratiques,
**afin de** séparer les workloads et maîtriser les coûts.

**Critères d'acceptation :**
- Base `FINTRACK_PROD` avec schémas `RAW`, `STAGING`, `MARTS_CORE`, `MARTS_FINANCE`, `MARTS_ANALYTICS`, `AUDIT`, `SNAPSHOTS`
- 5 warehouses dédiés (INGESTION, TRANSFORM, REPORTING, ADHOC, DEV) avec sizing et auto-suspend appropriés
- 5 rôles séparés selon la matrice RACI
- Resource monitor mensuel avec seuils à 75/90/100%

Base fournie dans `scripts/snowflake/01_setup_infrastructure.sql` — **exécutez, ne modifiez que si nécessaire**.

### User Story 1.2 — Génération et chargement des données
**En tant que** Data Engineer,
**je veux** charger un dataset volumineux dans le RAW,
**afin de** disposer d'un environnement de test représentatif.

**Critères d'acceptation :**
- Générer un dataset scale `M` minimum (1M transactions) — scale `L` (10M) recommandé
- Charger via `PUT` + `COPY INTO` avec `ON_ERROR = CONTINUE`
- Documenter les temps de chargement et le nombre d'erreurs dans `docs/PERF_LOG.md`

Scripts fournis : `scripts/python/generate_data.py` + `scripts/snowflake/02_ddl_raw_tables.sql` + `03_stage_and_copy.sql`.

**Livrable :** un `docs/PERF_LOG.md` documentant vos temps de chargement à différents scales.

---

## Sprint 2 — Modélisation core & double-écriture (Jour 3-4)

### User Story 2.1 — Compléter les modèles marqués TODO
**En tant que** Data Engineer,
**je veux** implémenter les modèles core manquants,
**afin de** disposer d'une modélisation Kimball propre.

**Critères d'acceptation :**
- `dim_categories` : compléter la hiérarchie récursive (CTE `WITH RECURSIVE`) — colonnes `chemin_complet`, `id_racine`
- `fct_virements` : implémenter la double-écriture (une ligne sortante + une ligne entrante par virement)
- Ajouter les tests unitaires dbt correspondants dans `unit_tests/`

### User Story 2.2 — Bridge table comptes ↔ titulaires
**En tant que** Data Analyst,
**je veux** connaître tous les titulaires d'un compte joint,
**afin de** ventiler les dépenses par personne physique.

**Critères d'acceptation :**
- Enrichir `generate_data.py` pour produire `raw_titulaires` et `raw_compte_titulaires`
- Ajouter les DDL correspondantes
- Implémenter la bridge `bridge_comptes_titulaires` avec `allocation_factor` et historisation

**Livrable :** modèles complétés + tests dbt passants (`dbt test --exclude tag:todo`).

---

## Sprint 3 — Incrémental et performance (Jour 5-6) — **CRITIQUE**

C'est le cœur de la mission. Vous devez ramener le temps d'exécution en dessous d'1h.

### User Story 3.1 — Stratégie incrémentale merge
**En tant que** Data Engineer,
**je veux** que `stg_transactions` et `fct_transactions` ne reprocessent que le delta,
**afin de** ramener le temps de run à moins de 15 min sur ces modèles.

**Critères d'acceptation :**
- Materialization `incremental` avec `incremental_strategy = merge`
- `unique_key` = `transaction_id`, `merge_update_columns` sur les colonnes qui peuvent changer (statut, is_reconciled, aml_*)
- Fenêtre de lookback paramétrable via `var('incremental_lookback_days')`
- `on_schema_change = 'append_new_columns'`

Squelettes déjà en place — **testez le comportement des mises à jour de statut** (une transaction `en_attente` qui passe `validee` doit être correctement merge).

### User Story 3.2 — Microbatch strategy (dbt 1.9+, expérimental)
**En tant que** Data Engineer,
**je veux** évaluer la stratégie `microbatch` de dbt 1.9,
**afin de** paralléliser le traitement des fenêtres temporelles.

**Critères d'acceptation :**
- Prototyper `fct_transactions_microbatch` avec `incremental_strategy = 'microbatch'`, `event_time`, `batch_size = 'day'`, `lookback = 3`
- Comparer les temps d'exécution `merge` vs `microbatch` (documenter dans PERF_LOG)
- Recommandation finale motivée

### User Story 3.3 — Clustering, Search Optimization, Query Profile
**En tant que** Data Engineer,
**je veux** optimiser le pruning et la latence des lookups,
**afin de** réduire les crédits consommés par les requêtes BI.

**Critères d'acceptation :**
- Analyser `SYSTEM$CLUSTERING_INFORMATION` sur `fct_transactions` — clustering depth < 3
- Évaluer l'ajout de Search Optimization sur `external_transaction_id` et `iban` (coût vs bénéfice)
- Capturer 3 QUERY_PROFILE de requêtes fréquentes et proposer des optimisations concrètes (dans `docs/PERF_LOG.md`)
- Ajuster les warehouses si nécessaire

**Livrable :**
- Modèles incrémentaux fonctionnels
- `docs/PERF_LOG.md` renseigné avec **temps avant/après**, screenshots Query Profile, décisions motivées

---

## Sprint 4 — Historisation & multi-devises (Jour 7)

### User Story 4.1 — SCD Type 2 sur les comptes
**En tant que** Analyst Compliance,
**je veux** l'historique complet des changements de KYC / statut / AML pour chaque compte,
**afin de** répondre aux exigences de piste d'audit réglementaire.

**Critères d'acceptation :**
- Le snapshot `snapshot_comptes` est fourni — l'exécuter et provoquer 3 changements de statut
- Implémenter `dim_comptes_scd2` avec `date_debut_validite`, `date_fin_validite`, `is_current`, `version_number`, surrogate key
- Ajouter une contrainte : chaque `compte_id` a exactement une version courante

### User Story 4.2 — FX historisés dans les faits
**En tant que** Analyst Finance,
**je veux** que le `montant_eur` de `fct_transactions` soit toujours calculé au taux du jour de la transaction,
**afin d'** éviter les divergences avec la comptabilité officielle.

**Critères d'acceptation :**
- Le modèle `int_fx_rates_daily` (fourni) fait le forward-fill des week-ends
- `fct_transactions` joint bien `int_fx_rates_daily` sur `(jour_transaction, devise)`
- Test : recalculer `montant_eur` d'un échantillon de 100 transactions et vérifier < 0.01% d'écart

**Livrable :** SCD2 fonctionnel + FX historisé validé.

---

## Sprint 5 — CI/CD & Orchestration (Jour 8)

### User Story 5.1 — GitHub Actions Slim CI
**En tant que** Data Engineer,
**je veux** qu'à chaque PR seuls les modèles modifiés (+ leurs downstream) soient reconstruits en CI,
**afin de** garder la CI < 10 min.

**Critères d'acceptation :**
- Workflow `.github/workflows/dbt_ci.yml` fourni — configurer les secrets GitHub
- `dbt build --select "state:modified+" --defer --state ../prod-artifacts`
- Chaque PR provisionne un schéma isolé `CI_<PR_ID>` via `generate_schema_name`
- Cleanup automatique en fin de job

### User Story 5.2 — Airflow DAG production
**En tant que** Ops,
**je veux** un DAG Airflow qui orchestre ingestion + dbt + reverse ETL,
**afin de** garantir la fraîcheur SLA.

**Critères d'acceptation :**
- Compléter `orchestration/airflow_dag_fintrack.py`
- Task groups : `ingestion`, `dbt`, `reverse_etl`
- Retries différenciés, SLA par task
- Notification Slack en cas d'échec (utiliser SlackWebhookOperator ou callback `on_failure_callback`)

**Livrable :** CI verte sur une PR de test + DAG déployable.

---

## Sprint 6 — Analytics avancés & bonus (Jour 9-10)

### User Story 6.1 — Analyses statistiques (TODO)
- `mart_cohortes_retention.sql` : compléter le calcul de rétention M+N
- `mart_transactions_anomalies.sql` : Z-score sur moyenne mobile 90 jours

### User Story 6.2 — Contracts et versioning
**En tant que** Producer de données,
**je veux** verrouiller le schéma de `fct_transactions` pour les consommateurs,
**afin de** ne plus casser leurs dashboards par inadvertance.

**Critères d'acceptation :**
- Activer `contract.enforced: true` sur `fct_transactions` (voir `_core.yml`)
- Créer une `fct_transactions_v2` en `versions:` avec un champ additionnel (ex : `montant_hors_taxes`)
- Documenter la stratégie de migration dans `docs/CONTRACTS.md`

### User Story 6.3 — Data Observability (bonus)
- Installer Elementary (`packages.yml`)
- Configurer le rapport HTML de monitoring
- Détecter au moins 1 anomalie de volumétrie

**Livrable :** tous les TODO résolus + rapport Elementary attaché.

---

## Livrables finaux

1. **Le repo dbt complet** avec tous les modèles fonctionnels et testés
2. **`docs/PERF_LOG.md`** — journal de performance avant/après optimisations (obligatoire)
3. **`docs/DESIGN.md`** — décisions d'architecture (matérialisations, clustering, contracts)
4. **`docs/CONTRACTS.md`** — stratégie de versioning des modèles publics
5. **Une PR de démonstration** sur GitHub avec CI verte
6. **Un rapport final** (2-3 pages) répondant :
   - Quels gains de performance concrets avez-vous obtenus ? (chiffres à l'appui)
   - Pourquoi merge plutôt que microbatch (ou l'inverse) dans votre cas ?
   - Quels sont les 3 principaux risques opérationnels de ce pipeline en production ?
   - Comment évoluerait ce projet pour absorber 10× le volume (1 milliard de transactions/mois) ?

---

## Ressources internes

- Slack : #data-platform, #fintrack-tech
- Wiki : https://wiki.fintrack.internal/data (fictif)
- On-call : PagerDuty (fictif)
- Snowflake account : à obtenir via SRE

## Ressources externes

- [dbt — Incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt — Microbatch strategy](https://docs.getdbt.com/docs/build/incremental-microbatch)
- [dbt — Contracts](https://docs.getdbt.com/docs/collaborate/govern/model-contracts)
- [dbt — Model versions](https://docs.getdbt.com/docs/collaborate/govern/model-versions)
- [Snowflake — Clustering](https://docs.snowflake.com/en/user-guide/tables-clustering-keys)
- [Snowflake — Search Optimization](https://docs.snowflake.com/en/user-guide/search-optimization-service)
- [Snowflake — QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/query_history)

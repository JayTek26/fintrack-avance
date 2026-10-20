# Design decisions — FinTrack Perf & Scale

Décisions d'architecture du projet dbt. Les chiffres cités viennent de `docs/PERF_LOG.md`.

## Modélisation

### Choix Kimball vs Data Vault
- **Kimball (étoile)** : faits `fct_transactions` / `fct_virements`, dimensions conformes `dim_tenants`, `dim_comptes` (+ `dim_comptes_scd2`), `dim_categories`, bridge `bridge_comptes_titulaires`.
- Data Vault écarté. Il y a une seule source (le core banking, déjà intégré par tenant), et les consommateurs sont des dashboards et du reverse ETL qui veulent des tables larges et stables. Le coût d'un Data Vault (hubs, links, satellites, puis une couche de présentation au-dessus) ne se justifie qu'avec des sources multiples et hétérogènes à réconcilier.
- La traçabilité réglementaire que Data Vault apporte est couverte autrement : snapshot SCD2 des comptes (KYC / statut / AML), RAW conservé en append-only, `_loaded_at` sur chaque ligne.

### Grain des tables de faits
- `fct_transactions` : une ligne = une transaction `validee` ou `en_attente`, non-reversal. Une transaction qui sort de ce périmètre après coup (rejet, annulation) est **supprimée** par le post-hook `purge_transactions_hors_perimetre`, car un MERGE seul la laisserait figée en `en_attente`.
- `fct_virements` : une ligne = une leg de virement (sortant OU entrant, donc 2 lignes par virement exécuté). Montants signés en devise et en EUR. Testé par `assert_virements_double_ecriture` (somme nulle, 2 legs) et par un test unitaire.
- `fct_transactions_microbatch` : prototype, même grain, désactivé par défaut.

### Bridge tables
- `bridge_comptes_titulaires` : relation many-to-many compte ↔ personne physique (comptes joints), à partir de `raw_titulaires` / `raw_compte_titulaires` (générateur enrichi au Sprint 2).
- `allocation_factor = part_pct / 100` sert à ventiler un montant par titulaire : `sum(montant_eur * allocation_factor)` par titulaire.
- Historisée par `date_debut` (incluse) / `date_fin` (exclue), avec `is_current`. Le test `assert_bridge_allocation_somme_a_1` vérifie qu'à chaque date la somme des facteurs actifs d'un compte vaut 1.
- Pour ventiler une transaction, joindre sur `compte_id` avec `date_debut <= jour_transaction < coalesce(date_fin, '9999-12-31')`.

### Montants en EUR
- `montant_eur` est **recalculé** dans `fct_transactions` et `fct_virements` au taux du jour : `int_fx_rates_daily`, joint sur `(jour_transaction, devise)`.
- Les taux sont cotés « 1 EUR = taux × devise », donc **division**. L'ancien code multipliait (un montant en JPY était multiplié par environ 173).
- Le `montant_eur` fourni par le core banking n'est pas utilisé : il est calculé avec un taux aléatoire (ratio ≈ 1,075 pour toutes les devises).
- `int_fx_rates_daily` fait le forward-fill jusqu'à aujourd'hui : les virements vont jusqu'à 10/2026, les cotations s'arrêtent au 31/12/2024. `date_cotation_source` indique la cotation réellement utilisée.
- Validation : `assert_fct_montant_eur_taux_du_jour` recalcule 100 transactions par `ASOF JOIN` direct sur `stg_fx_rates` (méthode indépendante du forward-fill) et exige un écart < 0,01 %.

## Matérialisations

| Modèle | Matérialisation | Raison |
|--------|-----------------|--------|
| stg_transactions | incremental (merge sur `transaction_id`) | 50-100M lignes, mises à jour de statut après insertion ; déduplication `QUALIFY ROW_NUMBER()` (RAW append-only) ; `event_time` déclaré pour les modèles microbatch aval |
| stg_virements | incremental (merge) | même logique, petit volume |
| autres stg_* | view | petits référentiels, pas de coût de stockage |
| int_transactions_normalisees | ephemeral | évite une table intermédiaire de 10M+ lignes ; le filtre incrémental de fct est poussé jusqu'à stg |
| int_fx_rates_daily | table | petit (≈ 14k lignes), lu par tous les faits |
| fct_transactions | incremental (merge), **contrat enforcé**, v1 | colonnes mises à jour ciblées via `merge_update_columns` ; purge du hors-périmètre en post-hook |
| fct_transactions_v2 | view sur la v1 | nouvelle version sans dupliquer le stockage |
| fct_virements | incremental (merge sur `virement_id, virement_leg`) | double écriture |
| fct_transactions_microbatch | incremental (microbatch), désactivé | prototype US 3.2, merge retenu (cf. PERF_LOG) |
| dim_comptes_scd2 | table | reconstruite depuis le snapshot (200k lignes, 3 s) |
| mart_solde_journalier | table | window function cumulée ; **candidat n°1 à l'incrémental** à 100M+ lignes (poste dominant du run incrémental) |
| mart_transactions_anomalies | table | fenêtre RANGE 90 jours par compte ; candidat n°2 à l'incrémental (recalcul des 90 derniers jours) |
| mart_cohortes_retention, mart_tenant_kpis_daily, mart_reconciliation | table | agrégats compacts |

Paramètres incrémentaux communs :
- `incremental_lookback_days` (défaut 1, sur `_loaded_at`) ;
- `on_schema_change = 'append_new_columns'` ;
- watermark = `max(_loaded_at)` de la cible.

## Clustering keys

| Table | Clustering key | Justification |
|-------|----------------|---------------|
| raw_transactions | (tenant_id, DATE_TRUNC('MONTH', date_transaction)) | hérité. **Recommandation :** la retirer, car le seul consommateur de RAW est l'extraction incrémentale sur `_loaded_at`, que l'ordre d'insertion naturel sert déjà |
| raw_comptes | (tenant_id, date_ouverture) | hérité, petite table |
| raw_fx_rates | (date_cotation) | hérité, toujours filtré par date |
| stg_transactions | (tenant_id, mois) | hérité. **Recommandation :** la retirer : premier poste d'auto-clustering (0,076 crédit le 20/10 en dev) sans requête qui en profite |
| fct_transactions | (tenant_id, mois) | conservée : tous les accès BI filtrent par tenant puis période (Q2 : 4 / 33 partitions) |
| dim_comptes | (tenant_id) | multi-tenant |
| dim_comptes_scd2 | (compte_id) | lookups d'historique par compte |
| mart_solde_journalier | (tenant_id, jour) | dashboards par tenant et date |
| mart_tenant_kpis_daily | (jour, tenant_id) | séries temporelles exécutives |
| mart_cohortes_retention | (tenant_id, cohort_mois) | |
| mart_transactions_anomalies | (tenant_id, jour_transaction) | revue d'alertes par tenant et date |

Search Optimization : `EQUALITY(external_transaction_id)` sur fct_transactions, en prod, via le post-hook `ensure_search_optimization` (survit aux `--full-refresh`).

## SCD Type 2

Pattern retenu pour `dim_comptes_scd2` : snapshot dbt `snapshot_comptes`, stratégie `check` avec `check_cols`.
- Colonnes surveillées : statut, kyc_level, aml_flag, email, type_compte, customer_segment, is_pep.
- `hard_deletes = 'invalidate'` : un compte supprimé de la source garde son historique, sans version courante.
- Snapshot écrit via `schema` (dbt ≥ 1.9), qui passe par `generate_schema_name` : SNAPSHOTS en prod, `DEV_<user>_snapshots` en dev, `CI_<PR>` en CI. L'ancien `target_schema` faisait écrire dev et CI dans le même schéma.
- Colonnes de la dimension : `compte_version_key` (surrogate key sur `compte_id` + `date_debut_validite`), `date_debut_validite`, `date_fin_validite` (NULL = courante), `is_current`, `version_number`.
- Contraintes testées :
  - `assert_scd2_une_version_courante` : exactement une version courante par compte présent dans la source ;
  - `assert_scd2_periodes_contigues` : versions sans trou ni chevauchement ;
  - `unique` sur la clé.
- Validé le 20/10/2026 sur 3 comptes : compte 7 actif → suspendu (AML suspicious) → actif ; compte 8 clôturé ; compte 10 passé en KYC advanced. Résultat : 200 004 versions pour 200 000 comptes.

## Tests

- **Tests unitaires** (`unit_tests/`, ajouté à `test-paths`) : signes des montants, exclusion des transactions de test, merge incrémental d'un changement de statut, double écriture des virements, hiérarchie récursive des catégories.
- **Fenêtrage des tests coûteux** : surcharge de `get_where_subquery`, placeholder `where: "__fenetre_incrementale__"` sur les tests des grosses tables. Complet avec `--vars '{tests_complets: true}'` (CI, DAG hebdomadaire).
- **`store_failures`** uniquement en prod (schéma AUDIT_TEST_FAILURES).
- **Observabilité** : Elementary, `volume_anomalies` sur fct_transactions (severity warn). Rapport : `docs/elementary/elementary_report.html`.

## Environnements

| Target | Base | Schémas | Sources RAW |
|--------|------|---------|-------------|
| dev | FINTRACK_DEV | `DEV_<USER>_<custom>` | clone zero-copy `FINTRACK_DEV.DEV_<USER>_RAW` |
| ci | FINTRACK_DEV | un seul schéma `CI_<PR_ID>`, supprimé en fin de job (`drop_ci_schema`) | FINTRACK_PROD.RAW, limité aux 30 derniers jours de transactions (`ci_lookback_days`) |
| prod | FINTRACK_PROD | STAGING, MARTS_CORE, MARTS_FINANCE, MARTS_ANALYTICS, SNAPSHOTS | FINTRACK_PROD.RAW |

Pour (re)créer le clone dev, à partir de `FINTRACK_DEV_ROLE` :

```sql
CREATE SCHEMA FINTRACK_DEV.DEV_<USER>_RAW;
CREATE OR REPLACE TABLE FINTRACK_DEV.DEV_<USER>_RAW.raw_transactions CLONE FINTRACK_PROD.RAW.raw_transactions;
-- … idem pour les 7 autres tables raw_*
```

## Convention de nommage

- `stg_<entity>` : staging
- `int_<action>_<entity>` : intermediate
- `dim_<entity>` : dimension
- `fct_<entity>` : fact
- `bridge_<entity1>_<entity2>` : bridge
- `mart_<domain>_<subject>` : mart consommable

## Query tags

Format : `team=<team>|project=<project>|target=<target>|model=<model>`

- Positionné via `on-run-start` (tag sans modèle) et par les materializations dbt-snowflake avant chaque modèle (le macro `set_query_tag` du projet surcharge celui de l'adapter et ajoute `model=`).
- Les coûts s'agrègent par modèle dans `ACCOUNT_USAGE.QUERY_HISTORY` avec `regexp_substr(query_tag, 'model=([a-z0-9_]+)', 1, 1, 'e')`.

## Compatibilité dbt Core / dbt Fusion

Le projet tourne sous dbt Fusion 2.0 (poste local) et sous dbt Core ≥ 1.10.5 (CI, pour la syntaxe `arguments:` des tests).

Spécificités Fusion :
- `latest_version_pointer: {enabled: false}` sur la v1 de fct_transactions (sinon Fusion crée une vue pointeur en conflit avec l'alias) ;
- `access` / `group` placés sous `config:`.

Particularité observée avec Fusion 2.0.8 : le test unitaire de `fct_virements` échoue en `dbt test` seul (schéma périmé en cache) mais passe en `dbt build`. C'est l'une des raisons pour lesquelles le DAG et la CI utilisent `dbt build`.

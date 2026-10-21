# FinTrack Perf & Scale — Rapport final

**Date :** 20/10/2026, mis à jour le 21/10/2026 (déploiement prod et CI) · **Périmètre :** sprints 1 à 6 · **Détails chiffrés :** `docs/PERF_LOG.md` · **Architecture :** `docs/DESIGN.md` · **Versioning :** `docs/CONTRACTS.md`

## Résumé

Le pipeline est désormais incrémental de bout en bout, testé et versionné. Il est livré avec une CI Slim et un DAG Airflow horaire. Deux défauts bloquants hérités ont été trouvés et corrigés en chemin :
- `_loaded_at` était NULL sur toute la RAW, donc **aucun incrémental ne traitait de ligne** ;
- `montant_eur` était **multiplié** par le taux au lieu d'être divisé (un montant en JPY était faux d'un facteur ≈ 30 000).

Toutes les mesures de performance ont été faites en dev (XSMALL, 10M transactions). Le pipeline est **déployé en prod depuis le 21/10/2026**, après le backfill de `_loaded_at` sur `FINTRACK_PROD.RAW`, et la **CI GitHub est verte** (PR #8). Il n'y a en revanche pas encore d'historique de runs en prod : les gains en prod restent des **extrapolations**, présentées comme telles.

## 1. Gains de performance obtenus

| Mesure (dev, XSMALL, scale L) | Avant | Après | Gain |
|---|---|---|---|
| Run complet, durée dbt (wall) | 3 min 27 s (full-refresh) | 2 min 00 s (incrémental horaire) | −42 % |
| Run complet, temps Snowflake | 347,5 s | 155,9 s | **−55 %** |
| Run complet, temps d'exécution Snowflake | 215,6 s | 72,3 s | **−66 %** |
| stg_transactions | 58,9 s, 11,9 Go de spill | 12,7 s, 0 spill | −78 % |
| fct_transactions, lignes réécrites | 9,45M (rebuild), puis 91 480 (lookback 7 j) | 28 112 (13 002 ins + 15 110 upd) | −83 % vs lookback 7 j |
| Suite de tests (`dbt test`) | 2 min 54 s, 16,6 Go scannés | 58 s, 5,3 Go | −67 % / −68 % |
| Lookup par `external_transaction_id` | 33 / 33 partitions | 1 / 33 (Search Optimization) | −84 % d'octets |
| Crédits par run (≈ wall × 1 crédit/h) | ≈ 0,058 | ≈ 0,033 | −43 % |

**Ce qui a produit ces gains**, du plus au moins rentable :
1. **Incrémental réparé.** `COPY INTO` avec `INCLUDE_METADATA`, et backfill dans un clone dev.
2. **Tests fenêtrés.** Sur un run incrémental, ils pesaient 66 % du temps warehouse. Ils sont maintenant restreints à la fenêtre incrémentale (surcharge de `get_where_subquery`), avec un run complet hebdomadaire et en CI.
3. **`store_failures` uniquement en prod.**
4. **Lookback 7 → 1 jour**, justifié par la monotonie de `_loaded_at`.
5. **Search Optimization** ciblée.

**Objectifs du CTO :**
- **Temps < 1 h (÷ 4).** Un run incrémental ne dépend plus de l'historique mais du delta (≈ 140k lignes/h à 100M transactions/mois), à deux exceptions près : le scan de la cible par le MERGE (0,86 Go pour 10M lignes, linéaire) et deux marts reconstruits en table. Extrapolation linéaire ×10 sur les postes qui dépendent du volume : environ 10 à 15 min par run sur WH_TRANSFORM SMALL, très en dessous de l'heure. Avec un run horaire, la fraîcheur gold (2 h) est tenue. **Non vérifié en prod.**
- **Coûts ÷ 2.** −43 % de crédits par run mesuré en dev, sans perte de fraîcheur (horaire contre quotidien auparavant). Les leviers warehouse restent à appliquer : WH_TRANSFORM MEDIUM multi-cluster → SMALL mono-cluster, REPORTING 5 → 2 clusters ECONOMY ; les commandes sont dans le PERF_LOG. À elles seules, elles divisent par 2 le coût horaire de WH_TRANSFORM, ce qui rend l'objectif atteignable. **À confirmer sur un mois de `WAREHOUSE_METERING_HISTORY` en prod.**
- **Industrialisation.**
  - Contrat enforcé et versions v1 / v2 sur `fct_transactions` ;
  - 119 tests de données et 6 tests unitaires, tous verts ;
  - Slim CI **verte sur GitHub** (PR #8, 1 min 05 : secrets, connexion, manifest de prod, `--defer`, nettoyage du schéma `CI_<PR>`) ; séquence complète également rejouée sous dbt Core 1.10 (182 PASS) ;
  - déploiement prod réalisé (`dbt build --target prod --full-refresh`) ;
  - DAG chargé sans erreur dans une `DagBag` Airflow 2.10 ;
  - Elementary installé, avec 2 anomalies de volumétrie détectées.

## 2. Pourquoi merge plutôt que microbatch

Mesuré sur le run quotidien, microbatch est **plus rapide sur le modèle seul** : 10,4 s contre 17,1 s, parce qu'il remplace 4 jours entiers sans joindre la cible. Nous retenons pourtant le **merge**, pour trois raisons :

1. **Exactitude.** Le cœur métier, ce sont des *mises à jour d'état* : statut, AML, rapprochement, sur des transactions de n'importe quel âge. Microbatch ne retraite que les fenêtres `[aujourd'hui − lookback, aujourd'hui]` de `date_transaction`. Sur notre simulation, les 50 changements de statut sur des transactions de 2023 sont captés par le merge (watermark sur `_loaded_at`) et ignorés par microbatch. Pour les capter, il faudrait un lookback égal à l'âge maximal d'une mise à jour, ce qui annule le gain.
2. **Clustering.** Chaque batch écrit des partitions qui mélangent tous les tenants. La profondeur passe à 36, contre 3,09 pour le merge, et le pruning des dashboards par tenant en souffre.
3. **Backfill.** 283 requêtes et 110 s pour un seul mois. L'historique complet représente environ 730 batchs, contre une requête pour le merge.

Microbatch reste le bon choix pour des faits **immuables** à forte volumétrie (événements applicatifs, cotations). Le prototype est conservé, désactivé (`enable_microbatch`).

## 3. Les 3 principaux risques opérationnels en production

1. **Intégrité du watermark `_loaded_at`.** Tout l'incrémental repose sur cette colonne. On l'a vu : un COPY mal configuré l'a laissée NULL sans qu'aucun run n'échoue. Les runs « réussissaient » à vide.
   *Atténuations :*
   - COPY par nom de colonne avec `INCLUDE_METADATA` ;
   - fraîcheur de source `error_after` en tête du DAG ;
   - test de volumétrie Elementary sur le fait ;
   - assertion `error_count = 0` après chaque COPY.

   - **fait le 21/10 :** backfill de `FINTRACK_PROD.RAW`, et tests `not_null` sur `_loaded_at` des sources `raw_transactions`, `raw_comptes` et `raw_virements`. Ils passent sur la RAW de prod en CI : c'est le garde-fou qui aurait détecté l'incident dès le 14/10.
2. **Mises à jour en place dans RAW et dérive du clustering.** Si l'ingestion fait des UPDATE au lieu d'append (upsert côté core banking), le pruning sur `_loaded_at` disparaît : partitions de 2023 avec un `_loaded_at` de 2025, profondeur 33 dans notre simulation. Le scan de RAW redevient complet à chaque run. Côté fait, chaque MERGE dégrade la profondeur (3,09 → 11,95) en attendant l'Automatic Clustering, payé en serverless.
   *Atténuations :*
   - contrat d'interface « RAW append-only » avec l'équipe ingestion ;
   - suivi de `AUTOMATIC_CLUSTERING_HISTORY` ;
   - retrait des clés de clustering inutiles (stg, raw).
3. **Changements de schéma et de règles en amont.** 80 colonnes core banking, 15 tenants, règles FX ou TVA métier.
   - Un nouveau champ passe (`append_new_columns`) ;
   - un renommage ou un changement de type casse le contrat de `fct_transactions` : c'est voulu, mais cela bloque le run horaire, donc le SLA ;
   - une cotation FX manquante rend `montant_eur` NULL et fait échouer le test `not_null`.

   *Atténuations :*
   - CI Slim sur chaque PR avec les tests complets ;
   - stratégie de versions documentée (`CONTRACTS.md`) ;
   - alertes Slack avec retries différenciés ;
   - `date_cotation_source` pour détecter les taux reportés trop longtemps (test d'âge à ajouter).

Risques secondaires :
- overhead dbt (≈ 50 % du wall-clock n'est pas du SQL) ;
- COMPUTE_WH utilisé hors query tags (2,8 crédits la semaine du 19/10) ;
- **divergence entre dbt Fusion (postes locaux) et dbt Core (CI).** Trois incompatibilités ne sont apparues qu'en CI : requête au parsing, override `is_incremental` des tests unitaires, étape `--empty`. Atténuation : valider sous dbt Core avant chaque PR, ou aligner tout le monde sur le même moteur.

## 4. Absorber 10× le volume (1 milliard de transactions / mois)

À ≈ 33M lignes/jour (≈ 1,4M/h), le fait atteint environ 12 milliards de lignes par an.

1. **Ingestion en continu.** Snowpipe (ou Snowpipe Streaming) à la place des PUT / COPY horaires : COPY par fichiers de 100-250 Mo, plus de goulot sur un poste client (le PUT du scale L prenait 2 h 15 depuis un portable). RAW strictement append-only, éventuellement en table Iceberg si d'autres moteurs doivent la lire.
2. **Borner le MERGE.** Le scan de la cible devient le poste dominant.
   - Extraire les mises à jour d'état dans un fait séparé (`fct_transactions_evenements`, append-only). L'historique devient alors immuable, ce qui rend le microbatch utilisable pour l'insertion.
   - À défaut, conserver le MERGE mais avec des `incremental_predicates` calculés depuis le batch (`date_transaction >= min(date_transaction du delta)`), après avoir vérifié que les mises à jour tardives restent rares.
   - Fait final reconstruit par une vue ou une dynamic table qui joint le dernier état.
3. **Dynamic tables** pour les marts en table (solde journalier, KPIs) avec `TARGET_LAG` aligné sur le tier (2h / 6h / 24h). La fraîcheur est alors gérée par Snowflake au lieu d'être reconstruite à chaque run. Sinon, passage en incrémental par partition de jours.
4. **Isolation par tier.**
   - Un warehouse de transformation par tier de SLA (gold horaire, silver et bronze toutes les 6 h) ;
   - `snowflake_warehouse` par modèle ;
   - un warehouse dédié et plus large pour les full-refresh (spill mesuré à 11,9 Go en XSMALL pour 10M lignes) ;
   - multi-cluster sur REPORTING uniquement.
5. **Clustering et pruning.**
   - À ce volume, la clé `(tenant_id, mois)` produit des partitions constantes (profondeur < 2 attendue) ;
   - envisager `(tenant_id, jour)` pour les requêtes de relevé ;
   - Search Optimization maintenue sur les identifiants de lookup ;
   - row access policies par tenant, qui imposent `tenant_id` dans chaque requête BI.
6. **Coûts et observabilité à l'échelle.**
   - Budgets Snowflake par warehouse ;
   - alertes sur `QUERY_HISTORY` par `model=` ;
   - Elementary sur les volumes par tenant (`dimensions: [tenant_id]`) pour détecter un tenant qui n'envoie plus rien, invisible dans le volume global ;
   - tests complets uniquement hebdomadaires, et échantillonnés au-delà.
7. **dbt.** Projet découpé par domaine (dbt Mesh : `core` public, `finance` et `analytics` consommateurs via `ref` inter-projets et contrats). Cela réduit le temps de parsing et d'orchestration, qui représente déjà ≈ 50 % du wall-clock aujourd'hui.

## Suites

Réalisé le 21/10/2026 :
- backfill de `_loaded_at` sur `FINTRACK_PROD.RAW`, confirmé par les tests `not_null` exécutés en CI sur la RAW de prod ;
- premier déploiement prod ;
- secrets GitHub et PR de démonstration avec CI verte (PR #8) ;
- captures Query Profile Snowsight (`docs/query_profiles/`, intégrées au PERF_LOG).

| Action restante | Responsable | Bloquant |
|---|---|---|
| Vérifier le watermark des tables incrémentales prod (`max(_loaded_at)` non NULL sur `FCT_TRANSACTIONS`) | Data Eng | à confirmer (sinon les runs incrémentaux ne traitent rien) |
| Application des réglages warehouses (PERF_LOG) — condition de l'objectif « coûts ÷ 2 » | SRE / ACCOUNTADMIN | non |
| Protection de `main` (« Require status checks to pass ») — les PR #1 à #6 ont été mergées CI rouge | Data Eng | non |
| Mesure des runs prod sur quelques semaines (`QUERY_HISTORY` par `model=`, `WAREHOUSE_METERING_HISTORY`) pour remplacer les extrapolations | Data Eng | non |
| Profondeur de clustering de `fct_transactions` en prod (cible < 3) | Data Eng | non |
| Validation finance des taux de TVA de la v2 | Finance | non |

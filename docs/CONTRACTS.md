# Model contracts & versioning

## Modèles publics (contract enforcé)

| Modèle | Version | Statut | Relation | Consommateurs |
|--------|---------|--------|----------|---------------|
| fct_transactions | **v1** | `latest_version`, contrat enforcé | `MARTS_CORE.FCT_TRANSACTIONS` (alias, nom historique conservé) | Metabase, Hightouch, Data Science |
| fct_transactions | **v2** | pré-release, contrat enforcé | `MARTS_CORE.FCT_TRANSACTIONS_V2` (vue sur la v1) | consommateurs volontaires (`ref('fct_transactions', v=2)`) |
| dim_comptes | v1 (draft) | non enforcé | `MARTS_CORE.DIM_COMPTES` | Metabase, CRM sync |
| mart_tenant_kpis_daily | v1 (draft) | non enforcé | `MARTS_ANALYTICS.MART_TENANT_KPIS_DAILY` | Dashboard exécutif |

Configuration de `fct_transactions` (`models/marts/core/_core.yml`) :
- `access: public`, `group: core` ;
- `contract.enforced: true` : les 43 colonnes sont déclarées avec leur `data_type`. dbt refuse de construire le modèle si le SQL produit une colonne en plus, en moins ou d'un autre type ;
- contraintes : `not_null` sur `transaction_id`, `tenant_id`, `compte_id`, `date_transaction` (appliquées par Snowflake), `primary_key` sur `transaction_id` (informative, non appliquée par Snowflake : l'unicité reste garantie par le MERGE et le test `unique`) ;
- `on_schema_change = 'append_new_columns'`, compatible avec un contrat sur un modèle incrémental.

### Ce que la v2 change

- Nouvelles colonnes :
  - `montant_hors_taxes_eur` = `montant_eur / (1 + taux_tva_applique)` ;
  - `taux_tva_applique` : taux standard du pays du marchand (FR 20 %, DE 19 %, ES / NL / BE 21 %, IT 22 %, LU 17 %, CH 8,1 %, GB 20 %, autres 20 %) ; NULL sans marchand.
- Hypothèse documentée : taux standard uniquement, pas de taux réduits par MCC. À valider avec la finance avant de promouvoir la v2.
- Implémentée en **vue au-dessus de la v1** (`ref('fct_transactions', v=1)`) : pas de second stockage des 50-100M lignes, ni de second MERGE. Si la v2 devait un jour changer le grain ou une clé, elle deviendrait une table à part entière.
- Ajouter une colonne n'est pas un changement cassant au sens dbt. La v2 sert ici à rendre la migration explicite pour les consommateurs. C'est le chemin qu'emprunterait un vrai changement cassant (renommage, changement de type, suppression).

## Ce qui casse un contrat (et impose une nouvelle version)

| Changement | Cassant ? | Démarche |
|------------|-----------|----------|
| Ajouter une colonne | non | ajout direct dans la version courante (le contrat est mis à jour dans la même PR) |
| Supprimer / renommer une colonne | **oui** | nouvelle version |
| Changer un type (ex. `number(18,2)` → `float`) | **oui** | nouvelle version |
| Changer le grain ou la clé | **oui** | nouvelle version, en table séparée |
| Changer une règle de calcul (ex. FX) | oui sur le fond, pas pour dbt | nouvelle version recommandée + communication |

En CI, dbt détecte les changements cassants sur un modèle à contrat non versionné : `state:modified` + `contract.enforced` → erreur « breaking change to contract ».

## Stratégie de migration

Quand casser le contrat d'un modèle public :

1. **Créer la nouvelle version** (`v: 2`) avec les changements, en pré-release. `latest_version` reste sur la v1, donc `ref('fct_transactions')` continue de pointer sur la v1 et aucun modèle aval ne bouge.
2. **Annoncer** : déclarer les consommateurs dans `exposures.yml` (dashboards Metabase, syncs Hightouch), publier dans #data-platform avec la liste des colonnes impactées et la date de bascule.
3. **Période de double run** (30 jours minimum) : les deux versions sont construites. Les consommateurs migrent un par un vers `ref('fct_transactions', v=2)` ou `FCT_TRANSACTIONS_V2`. Un test de cohérence v1 ↔ v2 (mêmes `transaction_id`, mêmes montants) tourne à chaque run.
4. **Bascule** : `latest_version: 2`, et `deprecation_date` posée sur la v1. dbt émet alors un warning à chaque `ref` vers la v1.
5. **Retrait** : à la date de dépréciation, suppression de la v1 et de sa relation. Si la v2 devient la table physique, renommer la relation `FCT_TRANSACTIONS` vers la v2 pendant une fenêtre de maintenance, pour que les requêtes SQL directes (Metabase natif) ne cassent pas.

## Planning proposé pour fct_transactions v2

| Étape | Date | Action |
|-------|------|--------|
| Pré-release | 20/10/2026 | v2 disponible en vue, `latest_version: 1` |
| Validation finance des taux de TVA | avant le 06/11/2026 | à défaut, la v2 reste en pré-release |
| Bascule | 20/11/2026 | `latest_version: 2`, `deprecation_date: 2026-12-31` sur la v1 |
| Retrait de la v1 | 31/12/2026 | suppression de la v1 |

## Configuration après bascule (cible)

```yaml
models:
  - name: fct_transactions
    latest_version: 2
    config:
      access: public
      group: core
      contract:
        enforced: true
    versions:
      - v: 1
        defined_in: fct_transactions
        deprecation_date: '2026-12-31'
        config:
          alias: fct_transactions
          latest_version_pointer:
            enabled: false
      - v: 2
        defined_in: fct_transactions_v2
        columns:
          - include: all
          - name: montant_hors_taxes_eur
            data_type: number(18,2)
          - name: taux_tva_applique
            data_type: number(5,4)
```

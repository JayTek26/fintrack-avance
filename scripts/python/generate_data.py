#!/usr/bin/env python3
"""
FinTrack Perf & Scale — Générateur de données synthétiques
============================================================

Génère un jeu de données réaliste pour la plateforme FinTrack B2B2C :
    - Tenants (banques partenaires)
    - Comptes clients
    - Transactions (table large, ~80 colonnes)
    - Virements internes
    - Taux de change historisés
    - Événements d'audit

Volumes paramétrables via --scale : XS (dev) → XXL (production simulation).

Sortie : fichiers CSV compressés (.csv.gz) dans data/raw/, prêts à être
chargés via COPY INTO depuis un stage Snowflake externe ou interne.

Usage :
    python generate_data.py --scale M --output data/raw/
    python generate_data.py --scale XL --output data/raw/ --seed 42

Presets de volume :
    XS  :    10 000 transactions      (dev rapide, ~1 min)
    S   :   100 000 transactions      (dev, ~3 min)
    M   : 1 000 000 transactions      (test perf, ~10 min)
    L   : 10 000 000 transactions     (bench, ~1h — nécessite ~8 Go RAM)
    XL  : 50 000 000 transactions     (simulation prod, ~5h)
    XXL : 100 000 000 transactions    (stress test, ~10h)
"""

import argparse
import csv
import gzip
import json
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

try:
    from faker import Faker
    import numpy as np
except ImportError:
    print("ERREUR : dépendances manquantes.")
    print("Installez avec : pip install faker numpy")
    sys.exit(1)


# ============================================================
# CONFIGURATION
# ============================================================

SCALE_PRESETS = {
    "XS":     {"n_transactions": 10_000,      "n_comptes": 200,      "n_tenants": 3},
    "S":      {"n_transactions": 100_000,     "n_comptes": 2_000,    "n_tenants": 5},
    "M":      {"n_transactions": 1_000_000,   "n_comptes": 20_000,   "n_tenants": 8},
    "L":      {"n_transactions": 10_000_000,  "n_comptes": 200_000,  "n_tenants": 12},
    "XL":     {"n_transactions": 50_000_000,  "n_comptes": 1_000_000,"n_tenants": 15},
    "XXL":    {"n_transactions": 100_000_000, "n_comptes": 2_000_000,"n_tenants": 20},
}

TENANTS_SEED = [
    ("BNP-FR",     "BNP Paribas France",           "FR", "EUR"),
    ("CA-FR",      "Crédit Agricole",              "FR", "EUR"),
    ("SG-FR",      "Société Générale",             "FR", "EUR"),
    ("ING-NL",     "ING Netherlands",              "NL", "EUR"),
    ("DB-DE",      "Deutsche Bank",                "DE", "EUR"),
    ("SANT-ES",    "Santander España",             "ES", "EUR"),
    ("UCG-IT",     "UniCredit Italia",             "IT", "EUR"),
    ("HSBC-UK",    "HSBC UK",                      "GB", "GBP"),
    ("BARC-UK",    "Barclays",                     "GB", "GBP"),
    ("UBS-CH",     "UBS Switzerland",              "CH", "CHF"),
    ("CS-CH",      "Credit Suisse",                "CH", "CHF"),
    ("KBC-BE",     "KBC Belgique",                 "BE", "EUR"),
    ("NORDEA-SE",  "Nordea Sweden",                "SE", "SEK"),
    ("DNB-NO",     "DNB Norway",                   "NO", "NOK"),
    ("REVOLUT",    "Revolut Business",             "GB", "GBP"),
    ("N26",        "N26",                          "DE", "EUR"),
    ("QONTO",      "Qonto",                        "FR", "EUR"),
    ("LYDIA",      "Lydia",                        "FR", "EUR"),
    ("BUNQ",       "bunq",                         "NL", "EUR"),
    ("STARLING",   "Starling Bank",                "GB", "GBP"),
]

DEVISES = ["EUR", "USD", "GBP", "CHF", "SEK", "NOK", "DKK", "PLN", "CZK", "JPY"]

CATEGORIES = [
    (1,  "Salaire",              "revenu",  "Revenus"),
    (2,  "Freelance",            "revenu",  "Revenus"),
    (3,  "Dividendes",           "revenu",  "Revenus"),
    (4,  "Alimentation",         "depense", "Quotidien"),
    (5,  "Transport",            "depense", "Quotidien"),
    (6,  "Carburant",            "depense", "Quotidien"),
    (7,  "Loyer",                "depense", "Logement"),
    (8,  "Électricité",          "depense", "Logement"),
    (9,  "Gaz",                  "depense", "Logement"),
    (10, "Internet",             "depense", "Logement"),
    (11, "Assurance habitation", "depense", "Logement"),
    (12, "Restaurant",           "depense", "Loisirs"),
    (13, "Bar",                  "depense", "Loisirs"),
    (14, "Cinéma",               "depense", "Loisirs"),
    (15, "Voyage",               "depense", "Loisirs"),
    (16, "Abonnements SVOD",     "depense", "Loisirs"),
    (17, "Épargne versée",       "depense", "Épargne"),
    (18, "PEA",                  "depense", "Épargne"),
    (19, "Assurance vie",        "depense", "Épargne"),
    (20, "Remboursement",        "revenu",  "Divers"),
    (21, "Santé",                "depense", "Quotidien"),
    (22, "Mutuelle",             "depense", "Quotidien"),
    (23, "Vêtements",            "depense", "Loisirs"),
    (24, "Éducation",            "depense", "Enfants"),
    (25, "Crèche",               "depense", "Enfants"),
    (26, "Impôts",               "depense", "Fiscal"),
    (27, "Cotisations sociales", "depense", "Fiscal"),
    (28, "Frais bancaires",      "depense", "Divers"),
    (29, "Intérêts crédit",      "depense", "Divers"),
    (30, "Cashback",             "revenu",  "Divers"),
]

CANAUX = ["web", "mobile_ios", "mobile_android", "api", "batch", "atm", "pos"]
DEVICES = ["desktop", "smartphone", "tablet", "atm", "pos_terminal", "server"]
STATUTS_TX = ["validee", "en_attente", "rejetee", "annulee", "en_cours_verification"]
STATUTS_TX_WEIGHTS = [0.92, 0.03, 0.02, 0.01, 0.02]
TYPES_OPERATION = ["debit", "credit"]
KYC_LEVELS = ["basic", "intermediate", "advanced", "not_verified"]
AML_FLAGS = ["clean", "watchlist_check", "manual_review", "suspicious", "cleared"]
AML_WEIGHTS = [0.985, 0.005, 0.005, 0.002, 0.003]
MOYENS_PAIEMENT = ["carte", "virement", "prelevement", "cheque", "especes", "wallet"]
MARCHANDS_CATS = ["retail", "food", "travel", "utilities", "healthcare", "education",
                  "entertainment", "financial_services", "government", "other"]


# ============================================================
# GÉNÉRATEURS
# ============================================================

def generate_tenants(n: int, output_dir: Path) -> list:
    """Table des tenants (banques partenaires B2B2C)."""
    path = output_dir / "raw_tenants.csv.gz"
    tenants = TENANTS_SEED[:n]
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tenant_id", "tenant_code", "tenant_name", "country_code",
                    "devise_locale", "date_onboarding", "sla_freshness_hours",
                    "contract_tier", "is_active"])
        for i, (code, name, country, devise) in enumerate(tenants, start=1):
            w.writerow([
                i, code, name, country, devise,
                (datetime(2021, 1, 1) + timedelta(days=random.randint(0, 1500))).date().isoformat(),
                random.choice([2, 4, 6, 12, 24]),
                random.choices(["gold", "silver", "bronze"], weights=[0.2, 0.5, 0.3])[0],
                random.random() > 0.05,
            ])
    print(f"  ✓ {path.name} ({n} tenants)")
    return list(range(1, n + 1))


def generate_categories(output_dir: Path) -> None:
    path = output_dir / "raw_categories.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["categorie_id", "nom_categorie", "type_categorie",
                    "groupe", "categorie_parent_id", "niveau_hierarchique",
                    "is_active", "date_creation"])
        for (cid, nom, type_c, groupe) in CATEGORIES:
            parent = None
            niveau = 1
            w.writerow([cid, nom, type_c, groupe, parent, niveau, True,
                        "2023-01-01"])
    print(f"  ✓ {path.name} ({len(CATEGORIES)} catégories)")


def generate_fx_rates(output_dir: Path, start_date: datetime, end_date: datetime) -> None:
    """Taux de change quotidiens historisés — nécessaire pour la multi-devises."""
    path = output_dir / "raw_fx_rates.csv.gz"
    n_days = (end_date - start_date).days + 1
    rows = 0
    base_rates = {
        "USD": 1.08, "GBP": 0.85, "CHF": 0.96, "SEK": 11.20,
        "NOK": 11.50, "DKK": 7.45, "PLN": 4.30, "CZK": 24.50, "JPY": 160.00,
    }
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["fx_id", "devise_source", "devise_cible", "date_cotation",
                    "taux", "source_provider", "loaded_at"])
        fx_id = 1
        for d in range(n_days):
            current = start_date + timedelta(days=d)
            for devise, base in base_rates.items():
                # marche aléatoire (random walk)
                noise = np.random.normal(0, base * 0.005)
                base_rates[devise] = max(0.01, base + noise)
                w.writerow([fx_id, "EUR", devise,
                            current.date().isoformat(),
                            round(base_rates[devise], 6),
                            random.choice(["ECB", "Reuters", "Bloomberg"]),
                            current.isoformat()])
                fx_id += 1
                rows += 1
    print(f"  ✓ {path.name} ({rows:,} lignes)")


def generate_comptes(n: int, tenant_ids: list, output_dir: Path, fake: Faker) -> list:
    """Comptes clients — schéma large avec KYC, adresse, préférences."""
    path = output_dir / "raw_comptes.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "compte_id", "tenant_id", "numero_compte", "iban", "bic",
            "nom_client", "prenom_client", "email", "telephone", "date_naissance",
            "adresse_ligne1", "adresse_ligne2", "code_postal", "ville", "pays",
            "type_compte", "devise", "solde_initial", "solde_actuel",
            "date_ouverture", "date_derniere_activite", "statut",
            "kyc_level", "kyc_date_verification", "aml_flag",
            "customer_segment", "preferred_language", "consent_marketing",
            "consent_data_sharing", "is_pep", "risk_score",
            "created_at", "updated_at", "created_by_source_system",
        ])
        for cid in range(1, n + 1):
            tenant = random.choice(tenant_ids)
            open_date = fake.date_between(start_date="-5y", end_date="-30d")
            last_act = fake.date_between(start_date=open_date, end_date="today")
            statut = random.choices(
                ["actif", "inactif", "cloture", "suspendu"],
                weights=[0.82, 0.10, 0.06, 0.02]
            )[0]
            w.writerow([
                cid, tenant, fake.bban(), fake.iban(), fake.swift(),
                fake.last_name(), fake.first_name(), fake.email(),
                fake.phone_number(), fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat(),
                fake.street_address(), fake.secondary_address() if random.random() < 0.3 else "",
                fake.postcode(), fake.city(), fake.country_code(),
                random.choices(["courant", "epargne", "joint", "pro", "jeune"],
                              weights=[0.55, 0.25, 0.10, 0.07, 0.03])[0],
                random.choices(DEVISES, weights=[0.55, 0.15, 0.10, 0.05, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02])[0],
                round(random.uniform(0, 15000), 2),
                round(random.uniform(-500, 50000), 2),
                open_date.isoformat(),
                last_act.isoformat(),
                statut,
                random.choices(KYC_LEVELS, weights=[0.30, 0.40, 0.25, 0.05])[0],
                fake.date_between(start_date=open_date, end_date="today").isoformat(),
                random.choices(AML_FLAGS, weights=AML_WEIGHTS)[0],
                random.choices(["mass_market", "affluent", "premium", "private"],
                              weights=[0.75, 0.18, 0.05, 0.02])[0],
                random.choices(["fr", "en", "de", "es", "it", "nl"],
                              weights=[0.50, 0.20, 0.10, 0.08, 0.07, 0.05])[0],
                random.random() > 0.4,
                random.random() > 0.6,
                random.random() < 0.008,
                round(random.uniform(0, 100), 1),
                fake.date_time_between(start_date=open_date, end_date="now").isoformat(),
                fake.date_time_this_year().isoformat(),
                random.choice(["core_banking_v1", "core_banking_v2", "onboarding_app", "legacy_import"]),
            ])
            if cid % 100_000 == 0:
                print(f"    ... {cid:,} comptes générés")
    print(f"  ✓ {path.name} ({n:,} comptes)")
    return list(range(1, n + 1))


def generate_transactions(n: int, compte_ids: list, tenant_ids: list,
                          output_dir: Path, fake: Faker, batch_size: int = 500_000) -> None:
    """Table de faits large — ~80 colonnes réalistes.

    Écrit en batches pour éviter de saturer la mémoire.
    """
    path = output_dir / "raw_transactions.csv.gz"
    columns = [
        # Identifiants
        "transaction_id", "external_transaction_id", "tenant_id", "compte_id",
        "compte_source_iban", "compte_dest_iban",
        # Temporalité (multiple timestamps — typique en finance)
        "date_transaction", "date_valeur", "date_comptabilisation",
        "date_reception", "date_traitement", "date_settlement",
        # Montants et devises
        "montant", "devise", "montant_eur", "taux_change_applique",
        "frais", "frais_devise", "montant_net",
        "type_operation", "sens",
        # Catégorisation
        "categorie_id", "categorie_auto_detectee", "confiance_categorie",
        "sous_categorie", "tags",
        # Marchand
        "marchand_nom", "marchand_id", "marchand_categorie", "marchand_pays",
        "marchand_mcc", "marchand_siret",
        # Moyen de paiement
        "moyen_paiement", "carte_id", "carte_last4", "carte_type",
        "carte_reseau", "authentification_3ds",
        # Contexte technique
        "canal", "device_type", "device_id", "user_agent",
        "ip_address", "session_id", "geolocation_lat", "geolocation_lon",
        # Statut et workflow
        "statut", "statut_precedent", "nb_tentatives", "date_derniere_tentative",
        "raison_rejet", "code_erreur", "message_erreur",
        # Audit et compliance
        "aml_score", "aml_flag", "aml_reviewed_by", "aml_review_date",
        "fraud_score", "fraud_rules_triggered", "is_flagged_for_review",
        # Rapprochement bancaire
        "is_reconciled", "date_reconciliation", "reconciliation_batch_id",
        "external_reference", "internal_reference",
        # Description
        "description", "description_brute", "description_enrichie",
        "libelle_court",
        # Métadonnées
        "source_system", "source_file_batch_id",
        "created_at", "updated_at", "loaded_at", "processing_version",
        "is_test", "is_reversal", "original_transaction_id",
    ]

    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns)

        # Pré-tirage vectorisé pour la perf
        n_batches = (n + batch_size - 1) // batch_size
        for b in range(n_batches):
            b_size = min(batch_size, n - b * batch_size)
            offset = b * batch_size

            # Tirages numpy en batch — beaucoup plus rapide
            tenants_arr = np.random.choice(tenant_ids, size=b_size)
            comptes_arr = np.random.choice(compte_ids, size=b_size)
            montants = np.round(np.random.lognormal(3.5, 1.2, b_size), 2)
            days_offsets = np.random.randint(0, 365 * 2, size=b_size)
            hour_offsets = np.random.randint(0, 24, size=b_size)
            minute_offsets = np.random.randint(0, 60, size=b_size)
            statuts = np.random.choice(STATUTS_TX, size=b_size, p=STATUTS_TX_WEIGHTS)
            types_op = np.random.choice(TYPES_OPERATION, size=b_size, p=[0.78, 0.22])
            categories_arr = np.random.choice(len(CATEGORIES), size=b_size) + 1
            devises_arr = np.random.choice(DEVISES, size=b_size,
                                           p=[0.55, 0.15, 0.10, 0.05, 0.03, 0.03, 0.03, 0.02, 0.02, 0.02])
            canaux_arr = np.random.choice(CANAUX, size=b_size,
                                          p=[0.20, 0.28, 0.30, 0.15, 0.04, 0.02, 0.01])
            aml_flags_arr = np.random.choice(AML_FLAGS, size=b_size, p=AML_WEIGHTS)
            aml_scores = np.round(np.random.beta(1, 30, b_size) * 100, 2)
            fraud_scores = np.round(np.random.beta(1, 50, b_size) * 100, 2)

            base_date = datetime(2023, 1, 1, tzinfo=timezone.utc)

            for i in range(b_size):
                tx_id = offset + i + 1
                dt = base_date + timedelta(days=int(days_offsets[i]),
                                           hours=int(hour_offsets[i]),
                                           minutes=int(minute_offsets[i]))
                # Dates multiples avec léger décalage (comme en vraie vie)
                dt_valeur = dt + timedelta(hours=random.randint(-2, 48))
                dt_compta = dt + timedelta(hours=random.randint(1, 72))
                dt_reception = dt + timedelta(minutes=random.randint(0, 5))
                dt_traitement = dt + timedelta(minutes=random.randint(5, 120))
                dt_settlement = dt + timedelta(days=random.randint(0, 3))

                montant = float(montants[i])
                devise = devises_arr[i]
                taux = 1.0 if devise == "EUR" else round(random.uniform(0.85, 1.30), 6)
                montant_eur = round(montant * taux, 2)
                frais = round(random.uniform(0, 2.5), 2) if random.random() < 0.15 else 0
                sens = "sortant" if types_op[i] == "debit" else "entrant"
                is_flagged = aml_flags_arr[i] in ("suspicious", "manual_review")

                w.writerow([
                    tx_id,
                    f"EXT-{uuid.uuid4().hex[:16].upper()}",
                    int(tenants_arr[i]),
                    int(comptes_arr[i]),
                    fake.iban() if random.random() < 0.4 else "",
                    fake.iban() if types_op[i] == "credit" and random.random() < 0.5 else "",
                    dt.isoformat(),
                    dt_valeur.isoformat(),
                    dt_compta.isoformat(),
                    dt_reception.isoformat(),
                    dt_traitement.isoformat(),
                    dt_settlement.isoformat(),
                    montant, devise, montant_eur, taux, frais, devise,
                    round(montant - frais, 2),
                    types_op[i], sens,
                    int(categories_arr[i]),
                    random.random() < 0.85,
                    round(random.uniform(0.3, 1.0), 3),
                    "" if random.random() < 0.6 else fake.word(),
                    "" if random.random() < 0.7 else ",".join(fake.words(nb=2)),
                    fake.company() if types_op[i] == "debit" else "",
                    f"MRC-{random.randint(1000, 99999)}" if types_op[i] == "debit" else "",
                    random.choice(MARCHANDS_CATS) if types_op[i] == "debit" else "",
                    fake.country_code() if types_op[i] == "debit" else "",
                    random.randint(1000, 9999) if types_op[i] == "debit" else "",
                    fake.numerify("###############") if random.random() < 0.3 else "",
                    random.choice(MOYENS_PAIEMENT),
                    f"CARD-{random.randint(10000, 99999)}" if random.random() < 0.7 else "",
                    fake.credit_card_number()[-4:] if random.random() < 0.7 else "",
                    random.choice(["visa", "mastercard", "amex", ""]) if random.random() < 0.7 else "",
                    random.choice(["visa_electron", "mastercard_debit", "amex_gold", ""]) if random.random() < 0.5 else "",
                    random.random() < 0.85,
                    canaux_arr[i],
                    random.choice(DEVICES),
                    f"DEV-{uuid.uuid4().hex[:12]}" if random.random() < 0.6 else "",
                    fake.user_agent() if random.random() < 0.5 else "",
                    fake.ipv4() if random.random() < 0.7 else "",
                    f"SESS-{uuid.uuid4().hex[:16]}" if random.random() < 0.4 else "",
                    round(fake.latitude(), 4) if random.random() < 0.3 else "",
                    round(fake.longitude(), 4) if random.random() < 0.3 else "",
                    statuts[i],
                    "en_attente" if statuts[i] != "en_attente" else "",
                    random.randint(1, 3) if statuts[i] in ("rejetee", "annulee") else 1,
                    dt_traitement.isoformat() if statuts[i] in ("rejetee", "annulee") else "",
                    random.choice(["insufficient_funds", "auth_failed", "expired_card",
                                   "fraud_suspected", "invalid_iban", ""]) if statuts[i] == "rejetee" else "",
                    f"ERR-{random.randint(100, 999)}" if statuts[i] == "rejetee" else "",
                    fake.sentence(nb_words=6) if statuts[i] == "rejetee" else "",
                    float(aml_scores[i]),
                    aml_flags_arr[i],
                    fake.user_name() if aml_flags_arr[i] != "clean" else "",
                    dt.isoformat() if aml_flags_arr[i] != "clean" else "",
                    float(fraud_scores[i]),
                    "|".join(random.sample(["velocity", "geo_anomaly", "amount_anomaly",
                                            "device_change", "known_fraudster"], k=random.randint(0, 2))),
                    is_flagged,
                    random.random() < 0.90,
                    dt_settlement.isoformat() if random.random() < 0.90 else "",
                    f"BATCH-{dt.strftime('%Y%m%d')}-{random.randint(1, 50)}" if random.random() < 0.90 else "",
                    f"REF-{uuid.uuid4().hex[:20].upper()}",
                    f"INT-{tx_id:012d}",
                    fake.sentence(nb_words=random.randint(3, 10)),
                    fake.text(max_nb_chars=80).replace("\n", " ")[:80],
                    fake.text(max_nb_chars=120).replace("\n", " ")[:120],
                    fake.text(max_nb_chars=30).replace("\n", " ")[:30],
                    random.choice(["core_banking_v1", "core_banking_v2",
                                   "payment_gateway", "batch_import"]),
                    f"FILE-{dt.strftime('%Y%m%d')}-{random.randint(1, 20)}",
                    dt_reception.isoformat(),
                    dt_traitement.isoformat(),
                    dt_traitement.isoformat(),
                    f"v{random.choice(['1.0', '1.1', '2.0', '2.1'])}",
                    False,
                    random.random() < 0.005,
                    tx_id - random.randint(1, 100) if random.random() < 0.005 else "",
                ])
            print(f"    ... batch {b+1}/{n_batches} — {(b+1)*batch_size:,}/{n:,} transactions")
    print(f"  ✓ {path.name} ({n:,} transactions, {len(columns)} colonnes)")


def generate_virements(n: int, compte_ids: list, tenant_ids: list,
                       output_dir: Path, fake: Faker) -> None:
    """Virements internes entre comptes."""
    path = output_dir / "raw_virements.csv.gz"
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["virement_id", "tenant_id", "compte_source_id", "compte_dest_id",
                    "montant", "devise", "date_virement", "date_execution",
                    "motif", "reference", "statut", "type_virement",
                    "frais", "created_at", "updated_at"])
        for i in range(1, n + 1):
            src, dst = random.sample(compte_ids, 2)
            dt = fake.date_time_between(start_date="-2y", end_date="now")
            w.writerow([
                i, random.choice(tenant_ids), src, dst,
                round(random.uniform(10, 5000), 2),
                random.choice(DEVISES[:3]),
                dt.isoformat(),
                (dt + timedelta(hours=random.randint(0, 48))).isoformat(),
                fake.sentence(nb_words=5),
                f"VIR-{uuid.uuid4().hex[:12].upper()}",
                random.choices(["execute", "en_attente", "annule", "rejete"],
                              weights=[0.88, 0.05, 0.04, 0.03])[0],
                random.choice(["ponctuel", "recurrent", "sepa", "swift", "instant"]),
                round(random.uniform(0, 5), 2) if random.random() < 0.3 else 0,
                dt.isoformat(),
                dt.isoformat(),
            ])
    print(f"  ✓ {path.name} ({n:,} virements)")


def generate_titulaires(compte_ids: list, output_dir: Path, fake: Faker) -> None:
    """Titulaires (personnes physiques) et liens compte <-> titulaires.

    ~85 % des comptes ont 1 titulaire, ~15 % sont joints (2 ou 3 titulaires).
    Pour ~5 % des comptes joints, le dernier co-titulaire est remplacé en cours
    de vie (historisation) : l'ancien lien est clos (date_fin), un nouveau est
    ouvert à la même date, et la somme des parts reste à 100 sur chaque période.
    Les intervalles sont semi-ouverts : [date_debut, date_fin[.
    """
    path_tit = output_dir / "raw_titulaires.csv.gz"
    path_lien = output_dir / "raw_compte_titulaires.csv.gz"
    pays = ["FR", "DE", "GB", "ES", "IT", "BE", "NL"]

    next_tid = 0
    n_joints = n_hist = n_liens = 0

    with gzip.open(path_tit, "wt", encoding="utf-8", newline="") as ft, \
         gzip.open(path_lien, "wt", encoding="utf-8", newline="") as fl:

        wt = csv.writer(ft)
        wl = csv.writer(fl)
        wt.writerow(["titulaire_id", "nom", "prenom", "date_naissance",
                     "pays_residence"])
        wl.writerow(["compte_id", "titulaire_id", "role_titulaire", "part_pct",
                     "date_debut", "date_fin"])

        def new_titulaire() -> int:
            nonlocal next_tid
            next_tid += 1
            wt.writerow([
                next_tid,
                fake.last_name(),
                fake.first_name(),
                fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
                random.choice(pays),
            ])
            return next_tid

        for compte_id in compte_ids:
            nb = 1 if random.random() > 0.15 else random.choice([2, 3])
            open_dt = fake.date_between(start_date="-2y", end_date="-90d")

            # Parts égales ; le 1er titulaire absorbe l'arrondi (somme = 100.00)
            parts = [round(100 / nb, 2)] * nb
            parts[0] = round(100 - sum(parts[1:]), 2)

            liens = []   # [titulaire_id, role, part, date_debut, date_fin]
            for i in range(nb):
                liens.append([new_titulaire(),
                              "PRINCIPAL" if i == 0 else "CO_TITULAIRE",
                              parts[i], open_dt, None])

            if nb > 1:
                n_joints += 1
                # Historisation : remplacement du dernier co-titulaire
                if random.random() < 0.05:
                    n_hist += 1
                    change_dt = fake.date_between(
                        start_date=open_dt + timedelta(days=30),
                        end_date="today")
                    ancien = liens[-1]
                    ancien[4] = change_dt                       # clôture
                    liens.append([new_titulaire(), "CO_TITULAIRE",
                                  ancien[2], change_dt, None])  # même part

            for tid, role, part, d_debut, d_fin in liens:
                wl.writerow([
                    compte_id, tid, role, part,
                    d_debut.isoformat(),
                    d_fin.isoformat() if d_fin else None,
                ])
                n_liens += 1

    print(f"  ✓ {path_tit.name} ({next_tid:,} titulaires)")
    print(f"  ✓ {path_lien.name} ({n_liens:,} liens, "
          f"{n_joints:,} comptes joints, {n_hist:,} avec historique)")

# ============================================================
# MAIN
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scale", choices=list(SCALE_PRESETS.keys()),
                        default="S", help="Volume preset (default: S)")
    parser.add_argument("--output", default="data/raw",
                        help="Répertoire de sortie (default: data/raw)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--tenants", type=int, help="Override n_tenants")
    parser.add_argument("--comptes", type=int, help="Override n_comptes")
    parser.add_argument("--transactions", type=int, help="Override n_transactions")
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)
    Faker.seed(args.seed)
    fake = Faker(["fr_FR", "en_GB", "de_DE"])

    preset = SCALE_PRESETS[args.scale].copy()
    if args.tenants:      preset["n_tenants"] = args.tenants
    if args.comptes:      preset["n_comptes"] = args.comptes
    if args.transactions: preset["n_transactions"] = args.transactions

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"FinTrack Perf & Scale — Génération de données")
    print("=" * 60)
    print(f"Scale         : {args.scale}")
    print(f"Tenants       : {preset['n_tenants']:>15,}")
    print(f"Comptes       : {preset['n_comptes']:>15,}")
    print(f"Transactions  : {preset['n_transactions']:>15,}")
    print(f"Virements     : {preset['n_comptes'] // 10:>15,}")
    print(f"Titulaires    : {'~1.2 x comptes':>15}")
    print(f"Output        : {output_dir}")
    print(f"Seed          : {args.seed}")
    print("=" * 60)

    start = datetime.now()

    print("\n[1/7] Tenants ...")
    tenant_ids = generate_tenants(preset["n_tenants"], output_dir)

    print("\n[2/7] Catégories ...")
    generate_categories(output_dir)

    print("\n[3/7] Taux FX (730 jours) ...")
    generate_fx_rates(output_dir,
                      datetime(2023, 1, 1),
                      datetime(2024, 12, 31))

    print("\n[4/7] Comptes ...")
    compte_ids = generate_comptes(preset["n_comptes"], tenant_ids, output_dir, fake)

    print("\n[5/7] Transactions ...")
    generate_transactions(preset["n_transactions"], compte_ids, tenant_ids,
                          output_dir, fake)

    print("\n[6/7] Virements ...")
    generate_virements(preset["n_comptes"] // 10, compte_ids, tenant_ids,
                       output_dir, fake)

    print("\n[7/7] Titulaires & liens compte-titulaires ...")
    generate_titulaires(compte_ids, output_dir, fake)

    duration = datetime.now() - start
    print("\n" + "=" * 60)
    print(f"✓ Génération terminée en {duration}")
    print(f"  Fichiers dans : {output_dir.resolve()}")
    print("=" * 60)
    print("\nProchaine étape : charger dans Snowflake via les scripts")
    print("  scripts/snowflake/03_stage_and_copy.sql")
    

if __name__ == "__main__":
    main()

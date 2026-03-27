"""
Bronze Layer - Ingestion des données brutes
Copie les fichiers sources tels quels dans data/bronze/ (source de vérité).
Pas de transformation — données brutes uniquement.
"""

import os
import shutil
from pathlib import Path


RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze")

# Mapping : fichier source -> nom de destination dans data/bronze/
RAW_FILES = {
    "CRIMES ET DELITS/donnee-dep-data.gouv-2025-geographie2025-produit-le2026-01-22.csv":
        "crimes_delits_raw.csv",
    "DEMANDEURS EMPLOIS/demandeurs_emplois_2019_2024.csv":
        "demandeurs_emplois_raw.csv",
    "ELECTIONS/resultats-definitifs-par-departement_2019.xls":
        "elections_2019_raw.xls",
    "ELECTIONS/resultats-definitifs-par-departement_2024.xlsx":
        "elections_2024_raw.xlsx",
    "IMPOTS/ircom_communes_complet_revenus_2019.xlsx":
        "revenus_fiscaux_2019_raw.xlsx",
    "IMPOTS/ircom_communes_complet_revenus_2024.xlsx":
        "revenus_fiscaux_2024_raw.xlsx",
    "NAISSANCES/Naissance_all.csv":
        "naissances_raw.csv",
    "POLICIERS MUNICIPAUX/effectifs-police-municipale-2019-.ods":
        "polices_municipaux_2019_raw.ods",
    "POLICIERS MUNICIPAUX/enquete-stats-pm-2024.ods":
        "polices_municipaux_2024_raw.ods",
    "RSA/rsa_2020.csv":
        "rsa_2020_raw.csv",
    "RSA/rsa_2024.csv":
        "rsa_2024_raw.csv",
    "TRANCHE AGE/pop_dep_age_sexe_2019.csv":
        "tranche_age_2019_raw.csv",
    "TRANCHE AGE/pop_dep_age_sexe_2024.csv":
        "tranche_age_2024_raw.csv",
    "TYPE LOGEMENTS/Donnees-annuelles-departementales-Logements.2026-02.csv":
        "type_logements_raw.csv",
}


def ingest_raw_data():
    """
    Copie les fichiers bruts vers data/bronze/ sans aucune transformation.
    """
    print("\n" + "=" * 70)
    print("Bronze LAYER - Ingestion des données brutes")
    print("=" * 70)

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    ingested = []

    for src_rel, dst_name in RAW_FILES.items():
        src = RAW_DIR / src_rel
        dst = BRONZE_DIR / dst_name
        if src.exists():
            shutil.copy2(src, dst)
            print(f"   OK {src_rel}  ->  bronze/{dst_name}")
            ingested.append(dst_name)
        else:
            print(f"   WARN  Introuvable : {src_rel}")

    print("\n" + "=" * 70)
    print(f"Bronze Layer termine - {len(ingested)} fichier(s) ingere(s)")
    print("=" * 70)
    return ingested


if __name__ == "__main__":
    ingest_raw_data()

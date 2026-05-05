"""
Silver Layer - Nettoyage et standardisation des donnees brutes
Traite tous les datasets sources et les depose dans data/silver/
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.silver.script_clean.process_elections import process_elections_data
from etl.silver.script_clean.process_trage import process_trage_data
from etl.silver.script_clean.process_impots import process_impots_data
from etl.silver.script_clean.process_dmd_emplois import process_dmd_emplois_data, process_dmd_emplois_2014_data
from etl.silver.script_clean.process_rsa import process_rsa_data
from etl.silver.script_clean.process_logements import process_logements_data
from etl.silver.script_clean.process_naissances import process_naissances_data, process_naissances_2014_data
from etl.silver.script_clean.process_polices_municipaux import process_polices_municipaux_data, process_polices_municipaux_2014_data
from etl.silver.script_clean.process_crimes import process_crimes, process_crimes_2014_data
from etl.silver.script_clean.process_population_tranche_2014 import process_population_tranche_2014_data


def process_all_datasets():
    """Orchestre le traitement de tous les datasets sources."""
    print("\n" + "=" * 70)
    print("SILVER LAYER - Nettoyage et standardisation des donnees")
    print("=" * 70)

    datasets_processed = []

    # --- Elections ---
    for year in [2024, 2019, 2014]:
        try:
            print(f"\n[INFO] process_elections.py (annee {year})")
            df = process_elections_data(year=year)
            if df is not None:
                datasets_processed.append(f"elections_{year}")
        except Exception as e:
            print(f"[ERREUR] elections {year}: {e}")

    # --- Tranches age ---
    for year in [2019, 2024]:
        try:
            print(f"\n[INFO] process_trage.py (annee {year})")
            df = process_trage_data(year=year)
            if df is not None:
                datasets_processed.append(f"tranche_age_{year}")
        except Exception as e:
            print(f"[ERREUR] tranche age {year}: {e}")

    # --- Revenus fiscaux 2019 / 2024 / 2014 ---
    for year in [2019, 2024, 2014]:
        try:
            print(f"\n[INFO] process_impots.py (annee {year})")
            df = process_impots_data(year=year)
            if df is not None:
                datasets_processed.append(f"revenus_fiscaux_{year}")
        except Exception as e:
            print(f"[ERREUR] revenus fiscaux {year}: {e}")

    # --- Demandeurs emploi ---
    try:
        print("\n[INFO] process_dmd_emplois.py")
        df = process_dmd_emplois_data()
        if df is not None:
            datasets_processed.append("demandeurs_emplois")
    except Exception as e:
        print(f"[ERREUR] demandeurs emplois: {e}")

    # --- Demandeurs emploi 2014 ---
    try:
        print("\n[INFO] process_dmd_emplois.py (annee 2014)")
        df = process_dmd_emplois_2014_data()
        if df is not None:
            datasets_processed.append("demandeurs_emplois_2014")
    except Exception as e:
        print(f"[ERREUR] demandeurs emplois 2014: {e}")

    # --- RSA ---
    for year in [2020, 2024]:
        try:
            print(f"\n[INFO] process_rsa.py (annee {year})")
            df = process_rsa_data(year=year)
            if df is not None:
                datasets_processed.append(f"rsa_{year}")
        except Exception as e:
            print(f"[ERREUR] RSA {year}: {e}")

    # --- Logements 2019 & 2024 ---
    try:
        print("\n[INFO] process_logements.py (annees 2019 & 2024)")
        df = process_logements_data()
        if df is not None:
            datasets_processed.append("type_logements")
    except Exception as e:
        print(f"[ERREUR] logements: {e}")

    # --- Logements 2014 ---
    try:
        print("\n[INFO] process_logements.py (annee 2014)")
        df = process_logements_data(
            input_file="data/raw/DATA 2014/TYPE LOGEMENTS/Donnees-annuelles-departementales-Logements.2026-02.csv",
            output_file="data/silver/type_logements_2014_par_departement.csv",
            years=[2014],
        )
        if df is not None:
            datasets_processed.append("type_logements_2014")
    except Exception as e:
        print(f"[ERREUR] logements 2014: {e}")

    # --- Naissances ---
    try:
        print("\n[INFO] process_naissances.py")
        df = process_naissances_data()
        if df is not None:
            datasets_processed.append("naissances")
    except Exception as e:
        print(f"[ERREUR] naissances: {e}")

    # --- Naissances 2014 ---
    try:
        print("\n[INFO] process_naissances.py (annee 2014)")
        df = process_naissances_2014_data()
        if df is not None:
            datasets_processed.append("naissances_2014")
    except Exception as e:
        print(f"[ERREUR] naissances 2014: {e}")

    # --- Police municipale 2019 & 2024 ---
    try:
        print("\n[INFO] process_polices_municipaux.py (annees 2019 & 2024)")
        df = process_polices_municipaux_data()
        if df is not None:
            datasets_processed.append("polices_municipaux")
    except Exception as e:
        print(f"[ERREUR] police municipale: {e}")

    # --- Police municipale 2014 ---
    try:
        print("\n[INFO] process_polices_municipaux.py (annee 2014)")
        df = process_polices_municipaux_2014_data()
        if df is not None:
            datasets_processed.append("polices_municipaux_2014")
    except Exception as e:
        print(f"[ERREUR] police municipale 2014: {e}")

    # --- Crimes et delits ---
    try:
        print("\n[INFO] process_crimes.py")
        df = process_crimes()
        if df is not None:
            datasets_processed.append("crimes_delits")
    except Exception as e:
        print(f"[ERREUR] crimes et delits: {e}")

    # --- Crimes et delits 2014 ---
    try:
        print("\n[INFO] process_crimes.py (annee 2014)")
        df = process_crimes_2014_data()
        if df is not None:
            datasets_processed.append("crimes_delits_2014")
    except Exception as e:
        print(f"[ERREUR] crimes et delits 2014: {e}")

    # --- Population tranche age 2014 ---
    try:
        print("\n[INFO] process_population_tranche_2014.py")
        df = process_population_tranche_2014_data()
        if df is not None:
            datasets_processed.append("population_tranche_2014")
    except Exception as e:
        print(f"[ERREUR] population tranche age 2014: {e}")

    print("\n" + "=" * 70)
    print(f"Silver Layer termine - {len(datasets_processed)} dataset(s) traite(s)")
    print("=" * 70)
    return datasets_processed


if __name__ == "__main__":
    process_all_datasets()

"""
Bronze Layer - Ingestion des données brutes
Traite tous les datasets sources et les dépose dans data/bronze/
"""

import pandas as pd
import os
import sys

# Ajouter le dossier parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importer les scripts de nettoyage
from etl.bronze.script_clean.process_elections import process_elections_data
from etl.bronze.script_clean.process_trage import process_trage_data
from etl.bronze.script_clean.process_impots import process_impots_data
from etl.bronze.script_clean.process_dmd_emplois import process_dmd_emplois_data
from etl.bronze.script_clean.process_rsa import process_rsa_data


def process_all_datasets():
    """
    Orchestre le traitement de tous les datasets sources
    et les dépose dans la couche bronze
    """
    print("\n" + "=" * 70)
    print("🥉 BRONZE LAYER - Ingestion des données brutes")
    print("=" * 70)
    
    datasets_processed = []
    
    # Traiter les élections 2024
    try:
        print("\n📥 Appel du script: process_elections.py (année 2024)")
        df_elections_2024 = process_elections_data(year=2024)
        if df_elections_2024 is not None:
            datasets_processed.append('elections_2024')
    except Exception as e:
        print(f"❌ Erreur lors du traitement des élections 2024: {e}")
    
    # Traiter les élections 2019
    try:
        print("\n📥 Appel du script: process_elections.py (année 2019)")
        df_elections_2019 = process_elections_data(year=2019)
        if df_elections_2019 is not None:
            datasets_processed.append('elections_2019')
    except Exception as e:
        print(f"❌ Erreur lors du traitement des élections 2019: {e}")

    # Traiter tranches d'âge 2019
    try:
        print("\n📥 Appel du script: process_trage.py (année 2019)")
        df_trage_2019 = process_trage_data(year=2019)
        if df_trage_2019 is not None:
            datasets_processed.append('tranche_age_2019')
    except Exception as e:
        print(f"❌ Erreur lors du traitement tranches d'âge 2019: {e}")

    # Traiter tranches d'âge 2024
    try:
        print("\n📥 Appel du script: process_trage.py (année 2024)")
        df_trage_2024 = process_trage_data(year=2024)
        if df_trage_2024 is not None:
            datasets_processed.append('tranche_age_2024')
    except Exception as e:
        print(f"❌ Erreur lors du traitement tranches d'âge 2024: {e}")

    # Traiter revenus fiscaux 2019
    try:
        print("\n📥 Appel du script: process_impots.py (année 2019)")
        df_impots_2019 = process_impots_data(year=2019)
        if df_impots_2019 is not None:
            datasets_processed.append('revenus_fiscaux_2019')
    except Exception as e:
        print(f"❌ Erreur lors du traitement revenus fiscaux 2019: {e}")

    # Traiter revenus fiscaux 2024
    try:
        print("\n📥 Appel du script: process_impots.py (année 2024)")
        df_impots_2024 = process_impots_data(year=2024)
        if df_impots_2024 is not None:
            datasets_processed.append('revenus_fiscaux_2024')
    except Exception as e:
        print(f"❌ Erreur lors du traitement revenus fiscaux 2024: {e}")

    # Traiter demandeurs d'emplois
    try:
        print("\n📥 Appel du script: process_dmd_emplois.py")
        df_dmd_emplois = process_dmd_emplois_data()
        if df_dmd_emplois is not None:
            datasets_processed.append('demandeurs_emplois')
    except Exception as e:
        print(f"❌ Erreur lors du traitement demandeurs d'emplois: {e}")

    # Traiter RSA 2020
    try:
        print("\n📥 Appel du script: process_rsa.py (année 2020)")
        df_rsa_2020 = process_rsa_data(year=2020)
        if df_rsa_2020 is not None:
            datasets_processed.append('rsa_2020')
    except Exception as e:
        print(f"❌ Erreur lors du traitement RSA 2020: {e}")

    # Traiter RSA 2024
    try:
        print("\n📥 Appel du script: process_rsa.py (année 2024)")
        df_rsa_2024 = process_rsa_data(year=2024)
        if df_rsa_2024 is not None:
            datasets_processed.append('rsa_2024')
    except Exception as e:
        print(f"❌ Erreur lors du traitement RSA 2024: {e}")
    
    # TODO: Ajouter d'autres datasets ici
    # df_dataset2 = process_dataset2()
    # df_dataset3 = process_dataset3()
    
    print("\n" + "=" * 70)
    print(f"✅ Bronze Layer terminé - {len(datasets_processed)} dataset(s) traité(s)")
    print("=" * 70)
    
    return datasets_processed


if __name__ == "__main__":
    process_all_datasets()

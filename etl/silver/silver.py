"""
Silver Layer - Nettoyage et standardisation des donnÃ©es brutes
Traite tous les datasets sources et les dÃ©pose dans data/silver/
"""

import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.silver.script_clean.process_elections import process_elections_data
from etl.silver.script_clean.process_trage import process_trage_data
from etl.silver.script_clean.process_impots import process_impots_data
from etl.silver.script_clean.process_dmd_emplois import process_dmd_emplois_data
from etl.silver.script_clean.process_rsa import process_rsa_data
from etl.silver.script_clean.process_logements import process_logements_data
from etl.silver.script_clean.process_naissances import process_naissances_data
from etl.silver.script_clean.process_polices_municipaux import process_polices_municipaux_data
from etl.silver.script_clean.process_crimes import process_crimes


def process_all_datasets():
    """
    Orchestre le traitement de tous les datasets sources
    et les dÃ©pose dans la couche silver
    """
    print("\n" + "=" * 70)
    print("ðŸ¥ˆ SILVER LAYER - Nettoyage et standardisation des donnÃ©es")
    print("=" * 70)

    datasets_processed = []

    # Traiter les Ã©lections 2024
    try:
        print("\nðŸ“¥ Appel du script: process_elections.py (annÃ©e 2024)")
        df_elections_2024 = process_elections_data(year=2024)
        if df_elections_2024 is not None:
            datasets_processed.append('elections_2024')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement des Ã©lections 2024: {e}")

    # Traiter les Ã©lections 2019
    try:
        print("\nðŸ“¥ Appel du script: process_elections.py (annÃ©e 2019)")
        df_elections_2019 = process_elections_data(year=2019)
        if df_elections_2019 is not None:
            datasets_processed.append('elections_2019')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement des Ã©lections 2019: {e}")

    # Traiter tranches d'Ã¢ge 2019
    try:
        print("\nðŸ“¥ Appel du script: process_trage.py (annÃ©e 2019)")
        df_trage_2019 = process_trage_data(year=2019)
        if df_trage_2019 is not None:
            datasets_processed.append('tranche_age_2019')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement tranches d'Ã¢ge 2019: {e}")

    # Traiter tranches d'Ã¢ge 2024
    try:
        print("\nðŸ“¥ Appel du script: process_trage.py (annÃ©e 2024)")
        df_trage_2024 = process_trage_data(year=2024)
        if df_trage_2024 is not None:
            datasets_processed.append('tranche_age_2024')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement tranches d'Ã¢ge 2024: {e}")

    # Traiter revenus fiscaux 2019
    try:
        print("\nðŸ“¥ Appel du script: process_impots.py (annÃ©e 2019)")
        df_impots_2019 = process_impots_data(year=2019)
        if df_impots_2019 is not None:
            datasets_processed.append('revenus_fiscaux_2019')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement revenus fiscaux 2019: {e}")

    # Traiter revenus fiscaux 2024
    try:
        print("\nðŸ“¥ Appel du script: process_impots.py (annÃ©e 2024)")
        df_impots_2024 = process_impots_data(year=2024)
        if df_impots_2024 is not None:
            datasets_processed.append('revenus_fiscaux_2024')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement revenus fiscaux 2024: {e}")

    # Traiter demandeurs d'emplois
    try:
        print("\nðŸ“¥ Appel du script: process_dmd_emplois.py")
        df_dmd_emplois = process_dmd_emplois_data()
        if df_dmd_emplois is not None:
            datasets_processed.append('demandeurs_emplois')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement demandeurs d'emplois: {e}")

    # Traiter RSA 2020
    try:
        print("\nðŸ“¥ Appel du script: process_rsa.py (annÃ©e 2020)")
        df_rsa_2020 = process_rsa_data(year=2020)
        if df_rsa_2020 is not None:
            datasets_processed.append('rsa_2020')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement RSA 2020: {e}")

    # Traiter RSA 2024
    try:
        print("\nðŸ“¥ Appel du script: process_rsa.py (annÃ©e 2024)")
        df_rsa_2024 = process_rsa_data(year=2024)
        if df_rsa_2024 is not None:
            datasets_processed.append('rsa_2024')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement RSA 2024: {e}")

    # Traiter les types de logements 2019 & 2024
    try:
        print("\nðŸ“¥ Appel du script: process_logements.py (annÃ©es 2019 & 2024)")
        df_logements = process_logements_data()
        if df_logements is not None:
            datasets_processed.append('type_logements')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement des types de logements: {e}")

    # Traiter les naissances 2019 & 2024
    try:
        print("\nðŸ“¥ Appel du script: process_naissances.py (annÃ©es 2019 & 2024)")
        df_naissances = process_naissances_data()
        if df_naissances is not None:
            datasets_processed.append('naissances')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement des naissances: {e}")

    # Traiter les effectifs de police municipale 2019 & 2024
    try:
        print("\nðŸ“¥ Appel du script: process_polices_municipaux.py (annÃ©es 2019 & 2024)")
        df_polices = process_polices_municipaux_data()
        if df_polices is not None:
            datasets_processed.append('polices_municipaux')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement des polices municipaux: {e}")

    # Traiter les crimes et dÃ©lits
    try:
        print("\nðŸ“¥ Appel du script: process_crimes.py")
        df_crimes = process_crimes()
        if df_crimes is not None:
            datasets_processed.append('crimes_delits')
    except Exception as e:
        print(f"âŒ Erreur lors du traitement des crimes et dÃ©lits: {e}")

    print("\n" + "=" * 70)
    print(f"âœ… Silver Layer terminÃ© - {len(datasets_processed)} dataset(s) traitÃ©(s)")
    print("=" * 70)

    return datasets_processed


if __name__ == "__main__":
    process_all_datasets()


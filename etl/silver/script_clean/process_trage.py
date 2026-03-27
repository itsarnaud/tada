"""
Script de traitement des populations par tranche d'âge
Agrège les âges en 4 catégories: 0/14, 15/29, 30/64, 64+
"""

import os
import pandas as pd


def process_trage_data(year=2019, input_file=None, output_file=None):
    """
    Traite les fichiers population par âge/sexes et produit 4 lignes par département.

    Colonnes de sortie: dep, dep_l, trage, annee, pop

    Args:
        year: Année des données (2019 ou 2024)
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
    """
    print("=" * 70)
    print(f"👥 Traitement des tranches d'âge {year}")
    print("=" * 70)

    if input_file is None:
        if year == 2019:
            input_file = 'data/raw/TRANCHE AGE/pop_dep_age_sexe_2019.csv'
        elif year == 2024:
            input_file = 'data/raw/TRANCHE AGE/pop_dep_age_sexe_2024.csv'
        else:
            raise ValueError(f"Année non supportée: {year}")

    if output_file is None:
        output_file = f'data/silver/tranche_age_{year}_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None

    print(f"\n📥 Chargement du fichier: {input_file}")
    df = pd.read_csv(input_file, sep=';', encoding='utf-8-sig')
    df.columns = [col.replace('\ufeff', '') for col in df.columns]

    print(f"   → {len(df)} lignes chargées")
    print(f"   → {len(df.columns)} colonnes trouvées")

    age_groups = {
        '0 à 4 ans': '0/14',
        '5 à 9 ans': '0/14',
        '10 à 14 ans': '0/14',
        '15 à 19 ans': '15/29',
        '20 à 24 ans': '15/29',
        '25 à 29 ans': '15/29',
        '30 à 34 ans': '30/64',
        '35 à 39 ans': '30/64',
        '40 à 44 ans': '30/64',
        '45 à 49 ans': '30/64',
        '50 à 54 ans': '30/64',
        '55 à 59 ans': '30/64',
        '60 à 64 ans': '30/64',
        '65 à 69 ans': '64+',
        '70 à 74 ans': '64+',
        '75 à 79 ans': '64+',
        '80 ans et plus': '64+'
    }

    df['trage_group'] = df['trage'].map(age_groups)
    df['Pop'] = pd.to_numeric(df['Pop'], errors='coerce').fillna(0)

    missing_groups = df['trage_group'].isna().sum()
    if missing_groups > 0:
        print(f"⚠️  {missing_groups} ligne(s) avec trage non mappé")

    grouped = (
        df.groupby(['dep', 'dep_l', 'trage_group'], as_index=False)['Pop']
        .sum()
    )

    grouped['annee'] = year

    # Pivoter pour avoir une ligne par département avec les 4 tranches d'âge en colonnes
    df_pivot = grouped.pivot_table(
        index=['dep', 'dep_l', 'annee'],
        columns='trage_group',
        values='Pop',
        aggfunc='sum'
    ).reset_index()

    # Renommer les colonnes pour avoir pop_0_14, pop_15_29, pop_30_64, pop_64plus
    df_pivot.columns.name = None
    column_mapping = {
        '0/14': 'pop_0_14',
        '15/29': 'pop_15_29',
        '30/64': 'pop_30_64',
        '64+': 'pop_64plus'
    }
    df_pivot = df_pivot.rename(columns=column_mapping)

    df_final = df_pivot[['dep', 'dep_l', 'annee', 'pop_0_14', 'pop_15_29', 'pop_30_64', 'pop_64plus']]

    print(f"\n💾 Sauvegarde des résultats...")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\n✅ Traitement terminé!")
    print(f"   → Fichier créé: {output_file}")
    print(f"   → {len(df_final)} lignes sauvegardées")
    print(f"   → Colonnes: {', '.join(df_final.columns)}")

    print("\n📊 Aperçu des résultats:")
    print(df_final.head(10).to_string(index=False))

    print("\n" + "=" * 70)

    return df_final


if __name__ == "__main__":
    process_trage_data(year=2019)
    process_trage_data(year=2024)

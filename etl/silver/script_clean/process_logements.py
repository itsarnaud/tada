"""
Script de traitement des types de logements par département
Filtre les années 2019 et 2024, calcule les % par type sur "Tous Logements"
"""

import os
import pandas as pd


def process_logements_data(input_file=None, output_file=None, years=None):
    """
    Traite le fichier CSV Types de Logements et produit une ligne par département par année.

    Colonnes de sortie:
        dep_libelle, dep_code, annee,
        pct_individuel_pur, pct_individuel_groupe, pct_collectif, pct_residence

    Les pourcentages sont calculés comme :
        LOG_AUT(catégorie) / LOG_AUT(Tous Logements) * 100

    Args:
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
        years: Liste des années à conserver (par défaut [2019, 2024])
    """
    if years is None:
        years = [2019, 2024]

    print("=" * 70)
    print(f"🏘️  Traitement des types de logements ({' & '.join(str(y) for y in years)})")
    print("=" * 70)

    if input_file is None:
        input_file = 'data/raw/TYPE LOGEMENTS/Donnees-annuelles-departementales-Logements.2026-02.csv'

    if output_file is None:
        output_file = 'data/silver/type_logements_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None

    print(f"\n📥 Chargement du fichier: {input_file}")
    df = pd.read_csv(input_file, sep=';', encoding='utf-8-sig')
    df.columns = [col.replace('\ufeff', '') for col in df.columns]

    print(f"   → {len(df)} lignes chargées")
    print(f"   → Années disponibles: {sorted(df['ANNEE'].unique())}")

    # Filtrer uniquement les années demandées
    df = df[df['ANNEE'].isin(years)].copy()
    print(f"   → {len(df)} lignes après filtre {years}")

    # S'assurer que LOG_AUT est numérique
    df['LOG_AUT'] = pd.to_numeric(df['LOG_AUT'], errors='coerce').fillna(0)

    # Séparer Tous Logements (dénominateur) et les catégories
    df_total = df[df['TYPE_LGT'] == 'Tous Logements'][
        ['ANNEE', 'DEPARTEMENT_CODE', 'DEPARTEMENT_LIBELLE', 'LOG_AUT']
    ].rename(columns={'LOG_AUT': 'total_log_aut'})

    categories = {
        'Individuel pur':    'pct_individuel_pur',
        'Individuel groupé': 'pct_individuel_groupe',
        'Collectif':         'pct_collectif',
        'Résidence':         'pct_residence',
    }

    # Pivoter les catégories en colonnes
    df_cat = df[df['TYPE_LGT'].isin(categories.keys())].copy()
    df_pivot = df_cat.pivot_table(
        index=['ANNEE', 'DEPARTEMENT_CODE', 'DEPARTEMENT_LIBELLE'],
        columns='TYPE_LGT',
        values='LOG_AUT',
        aggfunc='sum'
    ).reset_index()

    # Fusionner avec le total
    df_merged = df_pivot.merge(df_total, on=['ANNEE', 'DEPARTEMENT_CODE', 'DEPARTEMENT_LIBELLE'], how='left')

    # Calculer les pourcentages
    for type_lgt, col_name in categories.items():
        if type_lgt in df_merged.columns:
            df_merged[col_name] = (
                df_merged[type_lgt] / df_merged['total_log_aut'] * 100
            ).round(2)
        else:
            df_merged[col_name] = 0.0

    # Construire le DataFrame final
    df_final = df_merged[
        ['DEPARTEMENT_LIBELLE', 'DEPARTEMENT_CODE', 'ANNEE'] + list(categories.values())
    ].rename(columns={
        'DEPARTEMENT_LIBELLE': 'dep_libelle',
        'DEPARTEMENT_CODE':    'dep_code',
        'ANNEE':               'annee',
    })

    # Trier par département puis année
    df_final = df_final.sort_values(['dep_code', 'annee']).reset_index(drop=True)

    print(f"\n💾 Sauvegarde vers: {output_file}")
    df_final.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    print(f"   → {len(df_final)} lignes exportées")

    print("\n📊 Aperçu des premières lignes:")
    print(df_final.head(6).to_string(index=False))

    print("\n✅ Traitement des types de logements terminé")
    return df_final


if __name__ == "__main__":
    process_logements_data()

"""
Script de traitement des naissances par département
Filtre EC_MEASURE=LVB, FREQ=A, GEO_OBJECT=DEP, OBS_STATUS=D
Garde uniquement les années 2019 et 2024, une ligne par département par année
"""

import os
import pandas as pd


def process_naissances_data(input_file=None, output_file=None):
    """
    Traite le fichier CSV Naissances et produit une ligne par département par année.

    Colonnes de sortie: dep_code, annee, naissances

    Filtres appliqués:
        EC_MEASURE = LVB
        FREQ       = A
        GEO_OBJECT = DEP
        OBS_STATUS = D
        TIME_PERIOD in [2019, 2024]

    Args:
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
    """
    print("=" * 70)
    print("👶 Traitement des naissances par département (2019 & 2024)")
    print("=" * 70)

    if input_file is None:
        input_file = 'data/raw/NAISSANCES/Naissance_all.csv'

    if output_file is None:
        output_file = 'data/bronze/naissances_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None

    print(f"\n📥 Chargement du fichier: {input_file}")
    df = pd.read_csv(input_file, sep=';', encoding='utf-8-sig')
    df.columns = [col.replace('\ufeff', '') for col in df.columns]

    print(f"   → {len(df)} lignes chargées")

    # Appliquer les filtres
    mask = (
        (df['EC_MEASURE']   == 'LVB') &
        (df['FREQ']         == 'A')   &
        (df['GEO_OBJECT']   == 'DEP') &
        (df['OBS_STATUS_FR']== 'D')   &
        (df['TIME_PERIOD'].astype(str).isin(['2019', '2024']))
    )
    df_filt = df[mask].copy()
    print(f"   → {len(df_filt)} lignes après filtres (LVB / A / DEP / D / 2019-2024)")

    # Mise en forme
    df_filt['OBS_VALUE'] = pd.to_numeric(df_filt['OBS_VALUE'], errors='coerce').fillna(0).astype(int)

    df_final = df_filt[['GEO', 'TIME_PERIOD', 'OBS_VALUE']].rename(columns={
        'GEO':         'dep_code',
        'TIME_PERIOD': 'annee',
        'OBS_VALUE':   'naissances',
    })

    # Trier par département puis année
    df_final = df_final.sort_values(['dep_code', 'annee']).reset_index(drop=True)

    print(f"\n💾 Sauvegarde vers: {output_file}")
    df_final.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    print(f"   → {len(df_final)} lignes exportées")

    print("\n📊 Aperçu des premières lignes:")
    print(df_final.head(10).to_string(index=False))

    print("\n✅ Traitement des naissances terminé")
    return df_final


if __name__ == "__main__":
    process_naissances_data()

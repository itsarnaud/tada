"""
Script de traitement des demandeurs d'emploi
Agrège par année et département
"""

import os
import pandas as pd


def process_dmd_emplois_data(input_file=None, output_file=None):
    """
    Traite le fichier CSV des demandeurs d'emploi et agrège par année et département.
    
    Colonnes de sortie: annee, code_departement, nombre_demandeurs_emploi

    Args:
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
    """
    print("=" * 70)
    print(f"💼 Traitement des demandeurs d'emploi")
    print("=" * 70)

    if input_file is None:
        input_file = 'data/raw/DEMANDEURS EMPLOIS/demandeurs_emplois_2019_2024.csv'

    if output_file is None:
        output_file = 'data/silver/demandeurs_emplois_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None

    print(f"\n📥 Chargement du fichier: {input_file}")
    df = pd.read_csv(input_file, sep=';', encoding='utf-8-sig')
    
    print(f"   → {len(df)} lignes chargées")
    print(f"   → {len(df.columns)} colonnes trouvées")

    # Extraire l'année de la colonne Date (enlever -T4)
    df['annee'] = df['Date'].str.replace('-T4', '')
    
    # Convertir en numérique
    df['Nombre de demandeurs d\'emploi'] = pd.to_numeric(
        df['Nombre de demandeurs d\'emploi'], errors='coerce'
    ).fillna(0)

    print(f"\n🔢 Agrégation par année et département...")
    
    # Agréger par année et département
    df_agg = df.groupby(
        ['annee', 'Code département'], 
        as_index=False
    )['Nombre de demandeurs d\'emploi'].sum()
    
    # Renommer les colonnes
    df_agg.columns = ['annee', 'code_departement', 'nombre_demandeurs_emploi']
    
    # Arrondir à 2 chiffres (même si c'est des entiers, pour cohérence)
    df_agg['nombre_demandeurs_emploi'] = df_agg['nombre_demandeurs_emploi'].round(2)
    
    # Trier par département puis année
    df_final = df_agg.sort_values(['code_departement', 'annee']).reset_index(drop=True)

    print(f"\n💾 Sauvegarde des résultats...")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\n✅ Traitement terminé!")
    print(f"   → Fichier créé: {output_file}")
    print(f"   → {len(df_final)} lignes sauvegardées")
    print(f"   → Colonnes: {', '.join(df_final.columns)}")

    print("\n📊 Aperçu des résultats:")
    print(df_final.head(20).to_string(index=False))

    print("\n" + "=" * 70)

    return df_final


if __name__ == "__main__":
    process_dmd_emplois_data()

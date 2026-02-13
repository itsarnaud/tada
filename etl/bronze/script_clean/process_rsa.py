"""
Script de traitement des bénéficiaires RSA
Agrège par département (2 premiers chiffres du code commune)
"""

import os
import pandas as pd


def process_rsa_data(year=2020, input_file=None, output_file=None):
    """
    Traite le fichier CSV RSA et agrège par département.
    
    Colonnes de sortie: code_departement, nombre_personnes_rsa, annee

    Args:
        year: Année des données (2020 ou 2024)
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
    """
    print("=" * 70)
    print(f"🏠 Traitement des bénéficiaires RSA {year}")
    print("=" * 70)

    if input_file is None:
        input_file = f'data/raw/RSA/rsa_{year}.csv'

    if output_file is None:
        output_file = f'data/bronze/rsa_{year}_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None

    print(f"\n📥 Chargement du fichier: {input_file}")
    df = pd.read_csv(input_file, sep=';', encoding='utf-8-sig')
    
    print(f"   → {len(df)} lignes chargées")
    print(f"   → {len(df.columns)} colonnes trouvées")

    # Convertir le numéro de commune en string et extraire les 2 premiers chiffres
    df['Numéro commune'] = df['Numéro commune'].astype(str)
    
    # Extraire les 2 premiers caractères (département)
    # Si 5 chiffres : 73001 → 73
    # Si 4 chiffres : 7301 → 73 (mais attention, peut être 07)
    df['code_departement'] = df['Numéro commune'].str[:2]
    
    # Convertir les colonnes numériques
    df['Nombre foyers RSA_PPA'] = pd.to_numeric(
        df['Nombre foyers RSA_PPA'], errors='coerce'
    ).fillna(0)
    
    df['Nombre personnes RSA_PPA'] = pd.to_numeric(
        df['Nombre personnes RSA_PPA'], errors='coerce'
    ).fillna(0)

    print(f"\n🔢 Agrégation par département...")
    
    # Agréger par département
    df_agg = df.groupby('code_departement', as_index=False).agg({
        'Nombre personnes RSA_PPA': 'sum'
    })
    
    # Renommer les colonnes
    df_agg.columns = ['code_departement', 'nombre_personnes_rsa']
    
    # Ajouter l'année
    df_agg['annee'] = year
    
    # Réorganiser les colonnes
    df_final = df_agg[['code_departement', 'nombre_personnes_rsa', 'annee']]
    
    # Arrondir à 2 chiffres (même si entiers)
    df_final['nombre_personnes_rsa'] = df_final['nombre_personnes_rsa'].round(2)
    
    # Trier par département
    df_final = df_final.sort_values('code_departement').reset_index(drop=True)

    print(f"\n💾 Sauvegarde des résultats...")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\n✅ Traitement terminé!")
    print(f"   → Fichier créé: {output_file}")
    print(f"   → {len(df_final)} départements sauvegardés")
    print(f"   → Colonnes: {', '.join(df_final.columns)}")

    print("\n📊 Aperçu des résultats:")
    print(df_final.head(20).to_string(index=False))

    print("\n" + "=" * 70)

    return df_final


if __name__ == "__main__":
    process_rsa_data(year=2020)
    process_rsa_data(year=2024)

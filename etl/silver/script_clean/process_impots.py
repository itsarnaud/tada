"""
Script de traitement des revenus fiscaux par département
Agrège toutes les communes par département
"""

import os
import pandas as pd


def process_impots_data(year=2019, input_file=None, output_file=None):
    """
    Traite le fichier Excel des revenus fiscaux et agrège par département.
    
    Colonnes de sortie: dep, pct_foyers_imposes, revenu_fiscal_moyen_par_foyer,
                        ratio_actifs_retraites, annee

    Args:
        year: Année des données (2019 ou 2024)
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
    """
    print("=" * 70)
    print(f"💰 Traitement des revenus fiscaux {year}")
    print("=" * 70)

    if input_file is None:
        input_file = f'data/raw/IMPOTS/ircom_communes_complet_revenus_{year}.xlsx'

    if output_file is None:
        output_file = f'data/silver/revenus_fiscaux_{year}_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None

    print(f"\n📥 Chargement du fichier: {input_file}")
    
    # Structure différente selon l'année
    if year == 2019:
        # 2019: skiprows=5, colonnes avec header=None
        df = pd.read_excel(input_file, skiprows=5, header=None)
        df.columns = [
            'unused', 'dep', 'commune', 'libelle_commune', 'tranche',
            'nb_foyers_fiscaux', 'revenu_fiscal_reference', 'impot_net',
            'nb_foyers_imposes', 'revenu_fiscal_imposes',
            'nb_foyers_traitement_salaire', 'montant_traitement_salaire',
            'nb_foyers_retraite_pension', 'montant_retraite_pension'
        ]
        tranche_filter = 'TOTAL '
    else:  # 2024
        # 2024: skiprows=5, avec header=0
        df = pd.read_excel(input_file, skiprows=5, header=0)
        df.columns = [
            'dep', 'commune', 'libelle_commune', 'tranche',
            'nb_foyers_fiscaux', 'revenu_fiscal_reference', 'impot_net',
            'nb_foyers_imposes', 'revenu_fiscal_imposes',
            'nb_foyers_traitement_salaire', 'montant_traitement_salaire',
            'nb_foyers_retraite_pension', 'montant_retraite_pension'
        ]
        tranche_filter = 'Total'
    
    print(f"   → {len(df)} lignes chargées")
    
    # Filtrer uniquement les lignes TOTAL (pas les tranches)
    df_total = df[df['tranche'].astype(str).str.strip() == tranche_filter.strip()].copy()
    print(f"   → {len(df_total)} lignes avec TOTAL (pas de tranches)")
    
    # Nettoyer la colonne département (enlever espaces)
    df_total['dep'] = df_total['dep'].astype(str).str.strip()
    
    # Convertir les colonnes numériques
    numeric_cols = ['nb_foyers_fiscaux', 'revenu_fiscal_reference', 'impot_net', 
                    'nb_foyers_imposes', 'revenu_fiscal_imposes',
                    'nb_foyers_traitement_salaire', 'montant_traitement_salaire',
                    'nb_foyers_retraite_pension', 'montant_retraite_pension']
    
    for col in numeric_cols:
        df_total[col] = pd.to_numeric(df_total[col], errors='coerce').fillna(0)
    
    print(f"\n🔢 Agrégation par département...")
    
    # Agréger par département
    df_agg = df_total.groupby('dep', as_index=False)[numeric_cols].sum()
    
    # Calculer le pourcentage de foyers fiscaux imposés
    df_agg['pct_foyers_imposes'] = (df_agg['nb_foyers_imposes'] / df_agg['nb_foyers_fiscaux']) * 100
    
    # Calculer le revenu fiscal moyen par foyer
    df_agg['revenu_fiscal_moyen_par_foyer'] = df_agg['revenu_fiscal_reference'] / df_agg['nb_foyers_fiscaux']
    
    # Calculer le ratio Actifs/Retraités
    df_agg['ratio_actifs_retraites'] = df_agg['nb_foyers_traitement_salaire'] / df_agg['nb_foyers_retraite_pension']
    
    # Ajouter l'année
    df_agg['annee'] = year
    
    # Réorganiser les colonnes - garder uniquement les colonnes nécessaires
    df_final = df_agg[['dep', 'pct_foyers_imposes', 'revenu_fiscal_moyen_par_foyer',
                       'ratio_actifs_retraites', 'annee']].copy()
    
    # Arrondir à 2 chiffres après la virgule
    df_final['pct_foyers_imposes'] = df_final['pct_foyers_imposes'].round(2)
    df_final['revenu_fiscal_moyen_par_foyer'] = df_final['revenu_fiscal_moyen_par_foyer'].round(2)
    df_final['ratio_actifs_retraites'] = df_final['ratio_actifs_retraites'].round(2)
    
    print(f"\n💾 Sauvegarde des résultats...")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\n✅ Traitement terminé!")
    print(f"   → Fichier créé: {output_file}")
    print(f"   → {len(df_final)} départements sauvegardés")
    print(f"   → Colonnes: {', '.join(df_final.columns)}")

    print("\n📊 Aperçu des résultats:")
    print(df_final.head(10).to_string(index=False))

    print("\n" + "=" * 70)

    return df_final


if __name__ == "__main__":
    process_impots_data(year=2019)

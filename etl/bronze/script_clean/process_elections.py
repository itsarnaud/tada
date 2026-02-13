"""
Script de traitement des résultats électoraux par département
Extrait la liste gagnante (meilleur % voix/exprimés) pour chaque département
"""

import pandas as pd
import os


def process_elections_data(year=2024, input_file=None, output_file=None):
    """
    Traite le fichier Excel des élections et extrait:
    - Code département
    - Libellé département
    - Nuance de la liste gagnante
    - Libellé abrégé de la liste gagnante
    - Année
    
    Args:
        year: Année des élections (2019 ou 2024)
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
    """
    print("=" * 70)
    print(f"🗳️  Traitement des résultats électoraux {year}")
    print("=" * 70)
    
    # Chemins par défaut
    if input_file is None:
        if year == 2024:
            input_file = 'data/raw/ELECTIONS/resultats-definitifs-par-departement_2024.xlsx'
        elif year == 2019:
            input_file = 'data/raw/ELECTIONS/resultats-definitifs-par-departement_2019.xls'
        else:
            raise ValueError(f"Année non supportée: {year}")
    
    if output_file is None:
        output_file = f'data/bronze/elections_{year}_gagnants_par_departement.csv'
    
    # Créer le dossier de sortie si nécessaire
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Vérifier si le fichier existe
    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None
    
    print(f"\n📥 Chargement du fichier: {input_file}")
    df = pd.read_excel(input_file)
    print(f"   → {len(df)} départements chargés")
    print(f"   → {len(df.columns)} colonnes trouvées")
    
    # Liste pour stocker les résultats
    results = []
    
    print(f"\n🔍 Analyse de chaque département...")
    
    # Adapter les noms de colonnes selon l'année
    if year == 2019:
        # Pour 2019: format large avec colonnes répétées tous les 7 positions
        # Colonnes 16-22: N°Liste, Libellé Abrégé, Libellé Étendu, Nom Tête, Voix, % Voix/Ins, % Voix/Exp
        # Puis 23-29, 30-36, etc. (colonnes Unnamed)
        col_code_dept = 'Code du département'
        col_libelle_dept = 'Libellé du département'
        
        for index, row in df.iterrows():
            code_dept = row[col_code_dept]
            libelle_dept = row[col_libelle_dept]
            
            max_pct = -1
            best_liste = None
            
            # Première liste (colonnes nommées, positions 16-22)
            pct = row['% Voix/Exp']
            if pd.notna(pct) and pct > max_pct:
                max_pct = pct
                best_liste = row['Libellé Abrégé Liste']
            
            # Listes suivantes (colonnes Unnamed, cycle de 7 à partir de la position 23)
            for start_col in range(23, len(df.columns), 7):
                pct_col_idx = start_col + 6  # % Voix/Exp est à la position +6 dans le cycle
                if pct_col_idx < len(df.columns):
                    pct = row.iloc[pct_col_idx]
                    if pd.notna(pct) and pct > max_pct:
                        max_pct = pct
                        liste_col_idx = start_col + 1  # Libellé Abrégé est à la position +1
                        best_liste = row.iloc[liste_col_idx]
            
            results.append({
                'code_departement': code_dept,
                'libelle_departement': libelle_dept,
                'nuance_liste': None,  # Pas de nuance dans le fichier 2019
                'libelle_abrege_liste': best_liste,
                'pct_voix_exprimes': max_pct,
                'annee': year
            })
            
            print(f"   {code_dept:3} - {libelle_dept:30} → {best_liste} - {max_pct:.2f}%")
        
    else:  # 2024
        col_code_dept = 'Code département'
        col_libelle_dept = 'Libellé département'
        
        # Format large: une ligne par département avec colonnes répétées
        for index, row in df.iterrows():
            code_dept = row[col_code_dept]
            libelle_dept = row[col_libelle_dept]
            
            # Trouver la liste avec le plus haut % Voix/exprimés
            max_score = 0
            best_nuance = None
            best_libelle = None
            
            # Parcourir toutes les listes possibles (1 à 38)
            for i in range(1, 39):
                col_score = f'% Voix/exprimés {i}'
                col_nuance = f'Nuance liste {i}'
                col_libelle = f'Libellé abrégé de liste {i}'
                
                if col_score in df.columns:
                    # Récupérer le score (format: "XX,XX%")
                    score_str = str(row[col_score])
                    
                    # Vérifier si la valeur n'est pas NaN ou vide
                    if pd.notna(row[col_nuance]) and score_str not in ['nan', 'None', '']:
                        # Convertir le pourcentage en float
                        try:
                            # Enlever le % et remplacer la virgule par un point
                            score_value = float(score_str.replace('%', '').replace(',', '.'))
                            
                            if score_value > max_score:
                                max_score = score_value
                                best_nuance = row[col_nuance]
                                best_libelle = row[col_libelle]
                        except (ValueError, AttributeError):
                            continue
            
            # Ajouter le résultat
            results.append({
                'code_departement': code_dept,
                'libelle_departement': libelle_dept,
                'nuance_liste': best_nuance,
                'libelle_abrege_liste': best_libelle,
                'pct_voix_exprimes': max_score,
                'annee': year
            })
            
            print(f"   {code_dept:3} - {libelle_dept:30} → {best_nuance} ({best_libelle}) - {max_score:.2f}%")
    
    # Créer le DataFrame final
    df_final = pd.DataFrame(results)
    
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
    # Traiter les deux années
    process_elections_data(year=2024)
    process_elections_data(year=2019)

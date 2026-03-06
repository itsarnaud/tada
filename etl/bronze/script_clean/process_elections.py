"""
Script de traitement des résultats électoraux par département
Extrait la liste gagnante (meilleur % voix/exprimés) pour chaque département
"""

import pandas as pd
import os


# Partis classés à droite (Libellé Etendu Liste exact du fichier XLS 2019)
DROITE_PARTIES_2019 = {
    'ALLIANCE JAUNE, LA RÉVOLTE PAR LE VOTE',
    'ALLONS ENFANTS',
    'DÉMOCRATIE REPRÉSENTATIVE',
    "ENSEMBLE PATRIOTES ET GILETS JAUNES : POUR LA FRANCE, SORTONS DE L'UNION EUROPÉENNE !",
    'ENSEMBLE POUR LE FREXIT',
    'LA LIGNE CLAIRE',
    'LE COURAGE DE DÉFENDRE LES FRANÇAIS AVEC NICOLAS DUPONT-AIGNAN. DEBOUT LA FRANCE ! - CNIP',
    'LES EUROPÉENS',
    "LES OUBLIÉS DE L'EUROPE - ARTISANS, COMMERÇANTS, PROFESSIONS LIBÉRALES ET INDÉPENDANTS - ACPLI -",
    'LISTE DE LA RECONQUÊTE',
    "MOUVEMENT POUR L'INITIATIVE CITOYENNE",
    'NEUTRE ET ACTIF',
    'PACE - PARTI DES CITOYENS EUROPÉENS',
    'PARTI FÉDÉRALISTE EUROPÉEN - POUR UNE EUROPE QUI PROTÈGE SES CITOYENS',
    'PRENEZ LE POUVOIR, LISTE SOUTENUE PAR MARINE LE PEN',
    'RENAISSANCE SOUTENUE PAR LA RÉPUBLIQUE EN MARCHE, LE MODEM ET SES PARTENAIRES',
    'UDLEF (UNION DÉMOCRATIQUE POUR LA LIBERTÉ ÉGALITÉ FRATERNITÉ)',
    'UNE EUROPE AU SERVICE DES PEUPLES',
    "UNE FRANCE ROYALE AU COEUR DE L'EUROPE",
    'UNION DE LA DROITE ET DU CENTRE',
}


DROITE_PARTIES_2024 = {
    'AR',
    "BESOIN D'EUROPE",
    'DEFENDRE LES ENFANTS',
    'DEMOCRATIE REPRESENTATIVE',
    'FORTERESSE EUROPE',
    'FRANCE LIBRE',
    'HUMANITE SOUVERAINE',
    "L'EUROPE CA SUFFIT !",
    'LA DROITE POUR FAIRE ENTENDRE LA VOIX DE LA FRANCE EN EUROPE',
    'LA FRANCE FIERE, MENEE PAR MARION MARECHAL ET SOUTENUE PAR ÉRIC ZEMMOUR',
    'LIBERTÉ DÉMOCRATIQUE FRANÇAISE',
    'LISTE ASSELINEAU-FREXIT',
    'La FRANCE REVIENT',
    'NLP',
    'PACE',
    'POUR UNE AUTRE EUROPE',
    'POUR UNE DEMOCRATIE REELLE : DECIDONS NOUS-MEMES !',
    'PRENONS-NOUS EN MAIN',
}


def _parse_percent(value):
    if pd.isna(value):
        return None

    value_str = str(value).strip()
    if value_str in ['', 'nan', 'None']:
        return None

    try:
        return float(value_str.replace('%', '').replace(',', '.'))
    except (ValueError, AttributeError):
        return None


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
            sum_pct_droite = 0.0
            
            # Première liste (colonnes nommées, positions 16-22)
            # offset+0: N°Liste, +1: Libellé Abrégé, +2: Libellé Étendu, +3: Nom Tête, +4: Voix, +5: % Voix/Ins, +6: % Voix/Exp
            pct = row['% Voix/Exp']
            libelle_etendu = row['Libellé Etendu Liste']
            if pd.notna(pct) and pct > max_pct:
                max_pct = pct
                best_liste = row['Libellé Abrégé Liste']
            if pd.notna(libelle_etendu) and str(libelle_etendu) in DROITE_PARTIES_2019 and pd.notna(pct):
                sum_pct_droite += float(pct)
            
            # Listes suivantes (colonnes Unnamed, cycle de 7 à partir de la position 23)
            for start_col in range(23, len(df.columns), 7):
                pct_col_idx = start_col + 6      # % Voix/Exp est à la position +6 dans le cycle
                etendu_col_idx = start_col + 2   # Libellé Étendu est à la position +2 dans le cycle
                if pct_col_idx < len(df.columns):
                    pct = row.iloc[pct_col_idx]
                    libelle_etendu = row.iloc[etendu_col_idx]
                    if pd.notna(pct) and pct > max_pct:
                        max_pct = pct
                        liste_col_idx = start_col + 1  # Libellé Abrégé est à la position +1
                        best_liste = row.iloc[liste_col_idx]
                    if pd.notna(libelle_etendu) and str(libelle_etendu) in DROITE_PARTIES_2019 and pd.notna(pct):
                        sum_pct_droite += float(pct)
            
            results.append({
                'code_departement': code_dept,
                'libelle_departement': libelle_dept,
                'nuance_liste': None,  # Pas de nuance dans le fichier 2019
                'libelle_abrege_liste': best_liste,
                'pct_voix_exprimes': max_pct,
                '% vote droite': round(sum_pct_droite, 2),
                '% vote gauche': round((_parse_percent(row['% Exp/Vot']) or 0.0) - sum_pct_droite, 2),
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
            pct_exprimes_votants = _parse_percent(row['% Exprimés/votants']) or 0.0
            
            # Trouver la liste avec le plus haut % Voix/exprimés
            max_score = 0
            best_nuance = None
            best_libelle = None
            sum_pct_droite = 0.0
            
            # Parcourir toutes les listes possibles (1 à 38)
            for i in range(1, 39):
                col_score = f'% Voix/exprimés {i}'
                col_nuance = f'Nuance liste {i}'
                col_libelle = f'Libellé abrégé de liste {i}'
                
                if col_score in df.columns:
                    score_value = _parse_percent(row[col_score])
                    libelle_value = row[col_libelle]
                    
                    # Vérifier si la valeur n'est pas NaN ou vide
                    if pd.notna(row[col_nuance]) and score_value is not None:
                        if score_value > max_score:
                            max_score = score_value
                            best_nuance = row[col_nuance]
                            best_libelle = libelle_value

                        if pd.notna(libelle_value) and str(libelle_value) in DROITE_PARTIES_2024:
                            sum_pct_droite += score_value
            
            # Ajouter le résultat
            results.append({
                'code_departement': code_dept,
                'libelle_departement': libelle_dept,
                'nuance_liste': best_nuance,
                'libelle_abrege_liste': best_libelle,
                'pct_voix_exprimes': max_score,
                '% vote droite': round(sum_pct_droite, 2),
                '% vote gauche': round(pct_exprimes_votants - sum_pct_droite, 2),
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

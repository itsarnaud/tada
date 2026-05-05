"""
Script de traitement des résultats électoraux par département
Extrait la liste gagnante (meilleur % voix/exprimés) pour chaque département
"""

import pandas as pd
import os


# Classement politique des listes 2024 par numéro de liste (1-38)
MAPPING_BORDS_2024 = {
    1:  'EXTREME DROITE',  # Humanité Souveraine (Souverainiste)
    2:  'CENTRE',          # Pour une démocratie réelle (Citoyen)
    3:  'EXTREME DROITE',  # La France Fière (Reconquête)
    4:  'GAUCHE',          # La France Insoumise (LFI)
    5:  'EXTREME DROITE',  # La France Revient (RN)
    6:  'GAUCHE',          # Europe Écologie (EELV)
    7:  'EXTREME GAUCHE',  # Free Palestine
    8:  'GAUCHE',          # Parti Animaliste
    9:  'EXTREME GAUCHE',  # Parti Révolutionnaire Communistes
    10: 'CENTRE',          # Parti Pirate
    11: 'CENTRE',          # Besoin d'Europe (Renaissance/Majorité)
    12: 'CENTRE',          # PACE
    13: 'CENTRE',          # Équinoxe
    14: 'CENTRE',          # Écologie Positive et Territoires
    15: 'EXTREME DROITE',  # Liste Asselineau-Frexit
    16: 'EXTREME GAUCHE',  # Paix et Décroissance
    17: 'GAUCHE',          # Pour une autre Europe
    18: 'DROITE',          # La Droite (LR)
    19: 'EXTREME GAUCHE',  # Lutte Ouvrière
    20: 'GAUCHE',          # Changer l'Europe (Nouvelle Donne)
    21: 'GAUCHE',          # Nous le Peuple
    22: 'EXTREME GAUCHE',  # Urgence Révolution !
    23: 'EXTREME GAUCHE',  # PPL (Parti des Travailleurs)
    24: 'EXTREME DROITE',  # L'Europe ça suffit (Les Patriotes)
    25: 'EXTREME DROITE',  # Non ! Prenons-nous en mains
    26: 'EXTREME DROITE',  # Forteresse Europe
    27: 'GAUCHE',          # Réveiller l'Europe (PS - Place Publique)
    28: 'EXTREME GAUCHE',  # Non à l'UE et à l'Otan (PRCF)
    29: 'DROITE',          # Alliance Rurale (Jean Lassalle)
    30: 'EXTREME DROITE',  # France Libre
    31: 'GAUCHE',          # Europe Territoires Écologie (PRG)
    32: 'CENTRE',          # La Ruche Citoyenne
    33: 'GAUCHE',          # Gauche Unie (PCF)
    34: 'DROITE',          # Défendre les enfants
    35: 'CENTRE',          # Écologie au Centre
    36: 'CENTRE',          # Démocratie Représentative
    37: 'CENTRE',          # Espéranto
    38: 'DROITE',          # Liberté Démocratique Française
}


BORD_MAP_2019 = {
    # --- EXTRÊME GAUCHE ---
    'LUTTE OUVRIÈRE': 'EXTREME GAUCHE',
    'RÉVOLUTIONNAIRE': 'EXTREME GAUCHE',
    'DÉCROISSANCE 2019': 'EXTREME GAUCHE',

    # --- GAUCHE ---
    'LA FRANCE INSOUMISE': 'GAUCHE',
    "POUR L'EUROPE DES GENS": 'GAUCHE',
    'LISTE CITOYENNE': 'GAUCHE',
    "ENVIE D'EUROPE": 'GAUCHE',
    'EUROPE ÉCOLOGIE': 'GAUCHE',
    'URGENCE ÉCOLOGIE': 'GAUCHE',

    # --- CENTRE ---
    'RENAISSANCE': 'CENTRE',
    'LES EUROPÉENS': 'CENTRE',
    'PARTI PIRATE': 'CENTRE',
    'PARTI ANIMALISTE': 'CENTRE',
    'ALLIANCE JAUNE': 'CENTRE',
    'ÉVOLUTION CITOYENNE': 'CENTRE',
    'DÉMOCRATIE REPRÉSENTATIVE': 'CENTRE',
    'PACE': 'CENTRE',
    'PARTI FED. EUROPÉEN': 'CENTRE',
    'INITIATIVE CITOYENNE': 'CENTRE',
    'ALLONS ENFANTS': 'CENTRE',
    'À VOIX ÉGALES': 'CENTRE',
    'NEUTRE ET ACTIF': 'CENTRE',
    'ESPERANTO': 'CENTRE',
    "LES OUBLIES DE L'EUROPE": 'CENTRE',
    'UDLEF': 'CENTRE',
    'EUROPE AU SERVICE PEUPLES': 'CENTRE',

    # --- DROITE ---
    'UNION DROITE-CENTRE': 'DROITE',

    # --- EXTRÊME DROITE ---
    'PRENEZ LE POUVOIR': 'EXTREME DROITE',
    'DEBOUT LA FRANCE': 'EXTREME DROITE',
    'ENSEMBLE PATRIOTES': 'EXTREME DROITE',
    'ENSEMBLE POUR LE FREXIT': 'EXTREME DROITE',
    'LA LIGNE CLAIRE': 'EXTREME DROITE',
    'UNE FRANCE ROYALE': 'EXTREME DROITE',
    'LISTE DE LA RECONQUÊTE': 'EXTREME DROITE',
}


BORD_MAP_2014 = {
    # --- EXTRÊME GAUCHE ---
    'Liste Extrême gauche': 'EXTREME GAUCHE',

    # --- GAUCHE ---
    'Liste Front de Gauche': 'GAUCHE',
    'Liste Union de la Gauche': 'GAUCHE',
    'Liste Europe-Ecologie-Les Verts': 'GAUCHE',
    'Liste Divers gauche': 'GAUCHE',

    # --- CENTRE ---
    'Liste Union du Centre': 'CENTRE',
    'Liste Divers': 'CENTRE',

    # --- DROITE ---
    'Liste Union de la Droite': 'DROITE',
    'Liste Union pour un Mouvement Populaire': 'DROITE',  # UMP (nom dans ce fichier)
    'Liste Divers droite': 'DROITE',

    # --- EXTRÊME DROITE ---
    'Liste Front National': 'EXTREME DROITE',
    'Liste Extrême droite': 'EXTREME DROITE',
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
        year: Année des élections (2014, 2019 ou 2024)
        input_file: Chemin du fichier source (optionnel)
        output_file: Chemin du fichier de sortie (optionnel)
    """
    print("=" * 70)
    print(f"[ELECTIONS] Traitement des resultats electoraux {year}")
    print("=" * 70)
    
    # Chemins par défaut
    if input_file is None:
        if year == 2024:
            input_file = 'data/raw/ELECTIONS/resultats-definitifs-par-departement_2024.xlsx'
        elif year == 2019:
            input_file = 'data/raw/ELECTIONS/resultats-definitifs-par-departement_2019.xls'
        elif year == 2014:
            input_file = 'data/raw/DATA 2014/ELECTION/euro-2014-resultats-communes-c (1).xlsx'
        else:
            raise ValueError(f"Année non supportée: {year}")
    
    if output_file is None:
        output_file = f'data/silver/elections_{year}_gagnants_par_departement.csv'
    
    # Créer le dossier de sortie si nécessaire
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Vérifier si le fichier existe
    if not os.path.exists(input_file):
        print(f"[WARN] Fichier non trouve: {input_file}")
        return None
    
    print(f"\nChargement du fichier: {input_file}")
    df = pd.read_excel(input_file)
    print(f"   -> {len(df)} lignes chargees")
    print(f"   -> {len(df.columns)} colonnes trouvees")
    
    # Liste pour stocker les résultats
    results = []
    
    print(f"\nAnalyse de chaque departement...")
    
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
            best_bord = None
            pct_par_bord = {}
            
            # Première liste (colonnes nommées, positions 16-22)
            # offset+0: N°Liste, +1: Libellé Abrégé, +2: Libellé Étendu, +3: Nom Tête, +4: Voix, +5: % Voix/Ins, +6: % Voix/Exp
            pct = row['% Voix/Exp']
            libelle_abrege = row['Libellé Abrégé Liste']
            bord = BORD_MAP_2019.get(str(libelle_abrege), None) if pd.notna(libelle_abrege) else None
            if pd.notna(pct) and pct > max_pct:
                max_pct = pct
                best_liste = libelle_abrege
                best_bord = bord
            if bord and pd.notna(pct):
                pct_par_bord[bord] = pct_par_bord.get(bord, 0.0) + float(pct)
            
            # Listes suivantes (colonnes Unnamed, cycle de 7 à partir de la position 23)
            for start_col in range(23, len(df.columns), 7):
                pct_col_idx = start_col + 6      # % Voix/Exp est à la position +6 dans le cycle
                abrege_col_idx = start_col + 1   # Libellé Abrégé est à la position +1 dans le cycle
                if pct_col_idx < len(df.columns):
                    pct = row.iloc[pct_col_idx]
                    libelle_abrege = row.iloc[abrege_col_idx]
                    bord = BORD_MAP_2019.get(str(libelle_abrege), None) if pd.notna(libelle_abrege) else None
                    if pd.notna(pct) and pct > max_pct:
                        max_pct = pct
                        best_liste = libelle_abrege
                        best_bord = bord
                    if bord and pd.notna(pct):
                        pct_par_bord[bord] = pct_par_bord.get(bord, 0.0) + float(pct)
            
            results.append({
                'code_departement': code_dept,
                'libelle_departement': libelle_dept,
                'nuance_liste': None,  # Pas de nuance dans le fichier 2019
                'libelle_abrege_liste': best_liste,
                'bord_liste_gagnante': best_bord,
                'pct_voix_exprimes': max_pct,
                '% vote extreme gauche': round(pct_par_bord.get('EXTREME GAUCHE', 0.0), 2),
                '% vote gauche': round(pct_par_bord.get('GAUCHE', 0.0), 2),
                '% vote centre': round(pct_par_bord.get('CENTRE', 0.0), 2),
                '% vote droite': round(pct_par_bord.get('DROITE', 0.0), 2),
                '% vote extreme droite': round(pct_par_bord.get('EXTREME DROITE', 0.0), 2),
                'annee': year
            })
            
            print(f"   {code_dept:3} - {libelle_dept:30} -> {best_liste} - {max_pct:.2f}%")

    elif year == 2014:
        # Format par commune (une ligne = une commune), largeur variable avec suffixes .1, .2...
        # On agrège les voix par département puis on calcule les % sur le total d'exprimés.
        col_code_dept   = 'Code du département'
        col_libelle_dept = 'Libellé du département'
        col_exprimes    = 'Exprimés'

        # ── 1. Totaux exprimés par département ──────────────────────────────────
        dept_info = (
            df.groupby(col_code_dept)[col_exprimes]
            .sum()
            .reset_index()
            .rename(columns={col_code_dept: 'code_dept', col_exprimes: 'total_exprimes'})
        )
        dept_libelle = (
            df[[col_code_dept, col_libelle_dept]]
            .drop_duplicates(col_code_dept)
            .rename(columns={col_code_dept: 'code_dept', col_libelle_dept: 'libelle_dept'})
        )
        dept_info = dept_info.merge(dept_libelle, on='code_dept')

        # ── 2. Dépiler toutes les listes (format large → long) ───────────────────
        records = []

        # Première liste : colonnes sans suffixe
        sub = df[[col_code_dept, 'Libellé Abrégé Liste', 'Voix']].copy()
        sub.columns = ['code_dept', 'abrege', 'voix']
        records.append(sub.dropna(subset=['voix']))

        # Listes suivantes : colonnes avec suffixe .1, .2, ...
        suffix = 1
        while f'Voix.{suffix}' in df.columns:
            sub = df[[col_code_dept,
                       f'Libellé Abrégé Liste.{suffix}',
                       f'Voix.{suffix}']].copy()
            sub.columns = ['code_dept', 'abrege', 'voix']
            records.append(sub.dropna(subset=['voix']))
            suffix += 1

        votes_long = pd.concat(records, ignore_index=True)
        votes_long['voix'] = pd.to_numeric(votes_long['voix'], errors='coerce').fillna(0)

        # ── 3. Agrégation par (dépt, liste) puis calcul des % ─────────────────────
        votes_agg = (
            votes_long
            .groupby(['code_dept', 'abrege'], as_index=False)['voix']
            .sum()
            .merge(dept_info, on='code_dept')
        )
        votes_agg['pct'] = (votes_agg['voix'] / votes_agg['total_exprimes'] * 100).round(2)
        votes_agg['bord'] = votes_agg['abrege'].map(BORD_MAP_2014)

        # ── 4. % par bord par département ───────────────────────────────────────
        bord_pct = (
            votes_agg.dropna(subset=['bord'])
            .groupby(['code_dept', 'bord'], as_index=False)['pct']
            .sum()
            .pivot(index='code_dept', columns='bord', values='pct')
            .fillna(0)
            .reset_index()
        )
        bord_pct.columns.name = None

        # ── 5. Liste gagnante par département (max voix) ─────────────────────────
        winner_idx = votes_agg.groupby('code_dept')['voix'].idxmax()
        winner = votes_agg.loc[winner_idx, ['code_dept', 'libelle_dept', 'abrege', 'pct', 'bord']].copy()
        winner.rename(columns={
            'abrege': 'libelle_abrege_liste',
            'pct':    'pct_voix_exprimes',
            'bord':   'bord_liste_gagnante',
        }, inplace=True)

        # ── 6. Assemblage du résultat final ──────────────────────────────────────
        final = winner.merge(bord_pct, on='code_dept', how='left')

        for _, row in final.iterrows():
            # Normaliser le code département au format gold ('01'..'95', '2A', '2B', '971'...)
            raw_code = row['code_dept']
            try:
                dept_code = str(int(raw_code)).zfill(2)
            except (ValueError, TypeError):
                dept_code = str(raw_code)
            results.append({
                'code_departement':      dept_code,
                'libelle_departement':   row['libelle_dept'],
                'nuance_liste':          None,
                'libelle_abrege_liste':  row['libelle_abrege_liste'],
                'bord_liste_gagnante':   row.get('bord_liste_gagnante'),
                'pct_voix_exprimes':     round(row['pct_voix_exprimes'], 2),
                '% vote extreme gauche': round(row.get('EXTREME GAUCHE', 0.0), 2),
                '% vote gauche':         round(row.get('GAUCHE', 0.0), 2),
                '% vote centre':         round(row.get('CENTRE', 0.0), 2),
                '% vote droite':         round(row.get('DROITE', 0.0), 2),
                '% vote extreme droite': round(row.get('EXTREME DROITE', 0.0), 2),
                'annee': year
            })
            print(f"   {row['code_dept']:3} - {row['libelle_dept']:30} -> {row['libelle_abrege_liste']} - {row['pct_voix_exprimes']:.2f}%")

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
            best_bord = None
            pct_par_bord = {}
            
            # Parcourir toutes les listes possibles (1 à 38)
            for i in range(1, 39):
                col_score = f'% Voix/exprimés {i}'
                col_nuance = f'Nuance liste {i}'
                col_libelle = f'Libellé abrégé de liste {i}'
                
                if col_score in df.columns:
                    score_value = _parse_percent(row[col_score])
                    libelle_value = row[col_libelle]
                    bord = MAPPING_BORDS_2024.get(i, None)
                    
                    # Vérifier si la valeur n'est pas NaN ou vide
                    if pd.notna(row[col_nuance]) and score_value is not None:
                        if score_value > max_score:
                            max_score = score_value
                            best_nuance = row[col_nuance]
                            best_libelle = libelle_value
                            best_bord = bord

                        if bord:
                            pct_par_bord[bord] = pct_par_bord.get(bord, 0.0) + score_value
            
            # Ajouter le résultat
            results.append({
                'code_departement': code_dept,
                'libelle_departement': libelle_dept,
                'nuance_liste': best_nuance,
                'libelle_abrege_liste': best_libelle,
                'bord_liste_gagnante': best_bord,
                'pct_voix_exprimes': max_score,
                '% vote extreme gauche': round(pct_par_bord.get('EXTREME GAUCHE', 0.0), 2),
                '% vote gauche': round(pct_par_bord.get('GAUCHE', 0.0), 2),
                '% vote centre': round(pct_par_bord.get('CENTRE', 0.0), 2),
                '% vote droite': round(pct_par_bord.get('DROITE', 0.0), 2),
                '% vote extreme droite': round(pct_par_bord.get('EXTREME DROITE', 0.0), 2),
                'annee': year
            })
            
            print(f"   {code_dept:3} - {libelle_dept:30} -> {best_nuance} ({best_libelle}) - {max_score:.2f}%")
    
    # Créer le DataFrame final
    df_final = pd.DataFrame(results)
    
    print(f"\nSauvegarde des resultats...")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\nTraitement termine!")
    print(f"   -> Fichier cree: {output_file}")
    print(f"   -> {len(df_final)} lignes sauvegardees")
    print(f"   -> Colonnes: {', '.join(df_final.columns)}")
    
    print("\nApercu des resultats:")
    print(df_final.head(10).to_string(index=False))
    
    print("\n" + "=" * 70)
    
    return df_final


if __name__ == "__main__":
    # Traiter les trois années
    process_elections_data(year=2024)
    process_elections_data(year=2019)
    process_elections_data(year=2014)

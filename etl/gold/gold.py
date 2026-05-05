"""
Gold Layer - Consolidation de toutes les donnÃ©es silver
Produit un DataFrame unique : 202 lignes (101 depts Ã— 2 ans : 2019 & 2024)
Sortie : data/gold/departements_2019_2024.csv"""

import os
import pandas as pd
from sklearn.impute import SimpleImputer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=None, engine='python', encoding='utf-8-sig')


def _normalize_impots_dep(code) -> str | None:
    """
    Convertit le code dÃ©partement des revenus fiscaux vers le format standard.
    '010' -> '01', '2A0' -> '2A', '971' -> '971', 'B31' -> None (Ã  exclure)
    """
    s = str(code).strip()
    # DOM/ROM sans suffixe : 971, 972, 973, 974, 976
    if s in ('971', '972', '973', '974', '976'):
        return s
    # Codes avec suffixe '0' : strip le dernier caractÃ¨re
    if s.endswith('0') and len(s) >= 3:
        return s[:-1]
    return None  # ex: 'B31' â†’ ignorÃ©


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def process_gold_layer(output_file: str = None) -> pd.DataFrame | None:
    """
    Consolide tous les datasets silver en un seul DataFrame.

    Colonnes de sortie :
        dep_code, dep_libelle, annee,
        pop_0_14, pop_15_29, pop_30_64, pop_64plus, pop_total,
        taux_crimes_pour_mille,
        nombre_demandeurs_emploi,
        nuance_liste_gagnante, liste_gagnante, pct_voix_liste_gagnante,
        naissances,
        pct_policiers_municipaux,
        pct_foyers_imposes, revenu_fiscal_moyen_par_foyer, ratio_actifs_retraites,
        nombre_personnes_rsa,
        pct_individuel_pur, pct_individuel_groupe, pct_collectif, pct_residence
    """
    print("\n" + "=" * 70)
    print("ï¿½ GOLD LAYER - Consolidation des donnÃ©es")
    print("=" * 70)

    if output_file is None:
        output_file = 'data/gold/departements_2019_2024.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # â”€â”€ 1. BASE : tranches d'Ã¢ge (101 depts Ã— 2 ans = 202 lignes) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nðŸ“ Construction de la base (tranche_age 2019 & 2024)...")
    trage_2019 = _read('data/silver/tranche_age_2019_par_departement.csv')
    trage_2024 = _read('data/silver/tranche_age_2024_par_departement.csv')
    base = pd.concat([trage_2019, trage_2024], ignore_index=True)
    base.rename(columns={'dep': 'dep_code'}, inplace=True)
    # Extraire le libellÃ© propre depuis "01 - Ain" â†’ "Ain"
    base['dep_libelle'] = base['dep_l'].str.replace(r'^\d+[A-B]?\d* - ', '', regex=True).str.strip()
    base['dep_code'] = base['dep_code'].astype(str).str.strip()
    base['pop_total'] = base['pop_0_14'] + base['pop_15_29'] + base['pop_30_64'] + base['pop_64plus']
    base = base[['dep_code', 'dep_libelle', 'annee',
                 'pop_0_14', 'pop_15_29', 'pop_30_64', 'pop_64plus', 'pop_total']]
    print(f"   â†’ {len(base)} lignes de base ({base['dep_code'].nunique()} depts)")

    # â”€â”€ 2. CRIMES ET DÃ‰LITS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nðŸ”— Jointure crimes & dÃ©lits...")
    crimes = _read('data/silver/crimes_delits_par_departement.csv')
    crimes.rename(columns={
        'code_departement': 'dep_code',
        'taux_pour_mille': 'taux_crimes_pour_mille'
    }, inplace=True)
    crimes['dep_code'] = crimes['dep_code'].astype(str).str.strip()
    base = base.merge(
        crimes[['dep_code', 'annee', 'taux_crimes_pour_mille']],
        on=['dep_code', 'annee'], how='left'
    )

    # â”€â”€ 3. DEMANDEURS D'EMPLOI â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure demandeurs d'emploi...")
    dmd = _read('data/silver/demandeurs_emplois_par_departement.csv')
    dmd['code_departement'] = dmd['code_departement'].astype(str).str.strip()
    dmd = dmd[dmd['code_departement'] != 'Total']
    dmd.rename(columns={'code_departement': 'dep_code'}, inplace=True)
    base = base.merge(
        dmd[['dep_code', 'annee', 'nombre_demandeurs_emploi']],
        on=['dep_code', 'annee'], how='left'
    )

    # â”€â”€ 4. Ã‰LECTIONS (2019 + 2024 concatÃ©nÃ©s) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure Ã©lections...")
    elec_2019 = _read('data/silver/elections_2019_gagnants_par_departement.csv')
    elec_2024 = _read('data/silver/elections_2024_gagnants_par_departement.csv')
    elections = pd.concat([elec_2019, elec_2024], ignore_index=True)
    elections['code_departement'] = elections['code_departement'].astype(str).str.strip()
    elections.rename(columns={
        'code_departement':       'dep_code',
        'libelle_abrege_liste':   'liste_gagnante',
        'bord_liste_gagnante':    'nuance_liste_gagnante',
        'pct_voix_exprimes':      'pct_voix_liste_gagnante',
        '% vote extreme gauche':  'pct_vote_extreme_gauche',
        '% vote gauche':          'pct_vote_gauche',
        '% vote centre':          'pct_vote_centre',
        '% vote droite':          'pct_vote_droite',
        '% vote extreme droite':  'pct_vote_extreme_droite',
    }, inplace=True)

    elec_cols = ['dep_code', 'annee', 'nuance_liste_gagnante', 'pct_voix_liste_gagnante',
                 'pct_vote_extreme_gauche', 'pct_vote_gauche', 'pct_vote_centre',
                 'pct_vote_droite', 'pct_vote_extreme_droite']
    base = base.merge(
        elections[[c for c in elec_cols if c in elections.columns]],
        on=['dep_code', 'annee'], how='left'
    )
    base['nuance_liste_gagnante'] = base['nuance_liste_gagnante'].fillna('NON COMMUNIQUE')

    # â”€â”€ 5. NAISSANCES â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure naissances...")
    naiss = _read('data/silver/naissances_par_departement.csv')
    naiss['dep_code'] = naiss['dep_code'].astype(str).str.strip()
    base = base.merge(
        naiss[['dep_code', 'annee', 'naissances']],
        on=['dep_code', 'annee'], how='left'
    )

    # â”€â”€ 6. POLICIERS MUNICIPAUX â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure policiers municipaux...")
    police = _read('data/silver/polices_municipaux_par_departement.csv')
    police['dep_code'] = police['dep_code'].astype(str).str.strip()
    base = base.merge(
        police[['dep_code', 'annee', 'pct_policiers_municipaux']],
        on=['dep_code', 'annee'], how='left'
    )

    # â”€â”€ 7. REVENUS FISCAUX (2019 + 2024) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure revenus fiscaux...")
    rev_2019 = _read('data/silver/revenus_fiscaux_2019_par_departement.csv')
    rev_2024 = _read('data/silver/revenus_fiscaux_2024_par_departement.csv')
    revenus = pd.concat([rev_2019, rev_2024], ignore_index=True)
    revenus['dep_code'] = revenus['dep'].apply(_normalize_impots_dep)
    revenus = revenus[revenus['dep_code'].notna()]
    base = base.merge(
        revenus[['dep_code', 'annee',
                 'pct_foyers_imposes', 'revenu_fiscal_moyen_par_foyer',
                 'ratio_actifs_retraites']],
        on=['dep_code', 'annee'], how='left'
    )

    # â”€â”€ 8. RSA (2020 comme proxy 2019, + 2024) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure RSA...")
    rsa_2020 = _read('data/silver/rsa_2020_par_departement.csv')
    rsa_2024 = _read('data/silver/rsa_2024_par_departement.csv')
    # Renommer 2020 en 2019 (proxy)
    rsa_2020['annee'] = 2019
    rsa = pd.concat([rsa_2020, rsa_2024], ignore_index=True)
    rsa['code_departement'] = rsa['code_departement'].astype(str).str.strip()
    rsa = rsa[~rsa['code_departement'].isin(['97', 'na', 'nan', 'Total'])]
    rsa.rename(columns={'code_departement': 'dep_code'}, inplace=True)
    base = base.merge(
        rsa[['dep_code', 'annee', 'nombre_personnes_rsa']],
        on=['dep_code', 'annee'], how='left'
    )

    # â”€â”€ 9. TYPE DE LOGEMENTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure types de logements...")
    logements = _read('data/silver/type_logements_par_departement.csv')
    logements['dep_code'] = logements['dep_code'].astype(str).str.strip()
    base = base.merge(
        logements[['dep_code', 'annee',
                   'pct_individuel_pur', 'pct_individuel_groupe',
                   'pct_collectif', 'pct_residence']],
        on=['dep_code', 'annee'], how='left'
    )

    # â”€â”€ 10. NORMALISATION EN POURCENTAGES (base : pop_total) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ“ Conversion des effectifs en pourcentages (base: pop_total)...")
    for raw_col, pct_col in [
        ('pop_0_14',                 'pct_pop_0_14'),
        ('pop_15_29',                'pct_pop_15_29'),
        ('pop_30_64',                'pct_pop_30_64'),
        ('pop_64plus',               'pct_pop_64plus'),
        ('nombre_demandeurs_emploi', 'pct_demandeurs_emploi'),
        ('naissances',               'pct_naissances'),
        ('nombre_personnes_rsa',     'pct_personnes_rsa'),
    ]:
        base[pct_col] = (base[raw_col] / base['pop_total'] * 100).round(2)
        base.drop(columns=[raw_col], inplace=True)

    # â”€â”€ RÃ©sumÃ© de couverture â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print(f"\nðŸ“Š RÃ©sumÃ© ({len(base)} lignes Ã— {len(base.columns)} colonnes) :")
    data_cols = [c for c in base.columns if c not in ('dep_code', 'dep_libelle', 'annee')]
    for col in data_cols:
        n_null = base[col].isna().sum()
        coverage = round((1 - n_null / len(base)) * 100, 1)
        flag = "âœ…" if n_null == 0 else ("âš ï¸ " if n_null <= 10 else "âŒ")
        print(f"   {flag} {col:<40} {coverage:>5}% ({n_null} NaN)")

    # â”€â”€ Imputation des valeurs manquantes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nImputation des valeurs manquantes (mÃ©diane)...")
    numeric_cols = base.select_dtypes(include=['float64', 'int64']).columns.tolist()
    numeric_cols = [c for c in numeric_cols if c not in ('dep_code', 'annee')]
    
    if numeric_cols:
        imputer = SimpleImputer(strategy='median')
        base[numeric_cols] = pd.DataFrame(
            imputer.fit_transform(base[numeric_cols]),
            columns=numeric_cols
        )
        print(f"   â†’ Trous comblÃ©s avec la mÃ©diane ({len(numeric_cols)} colonnes numÃ©riques)")

    # â”€â”€ Sauvegarde â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print(f"\nðŸ’¾ Sauvegarde vers: {output_file}")
    base.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    print(f"   â†’ {len(base)} lignes, {len(base.columns)} colonnes")

    print("\nðŸ“‹ AperÃ§u (premiÃ¨res lignes) :")
    pd.set_option('display.max_columns', 5)
    pd.set_option('display.width', 100)
    print(base[['dep_code', 'dep_libelle', 'annee', 'pop_total', 'taux_crimes_pour_mille']].head(6).to_string(index=False))

    print("\nâœ… Gold Layer terminÃ©")
    return base


# ---------------------------------------------------------------------------
# Gold 2014
# ---------------------------------------------------------------------------

def process_gold_2014_layer(output_file: str = None) -> pd.DataFrame | None:
    """
    Consolide les donnÃ©es 2014 disponibles en un seul DataFrame.

    Sources utilisÃ©es (couche silver) :
        - revenus_fiscaux_2014_par_departement.csv
        - polices_municipaux_2014_par_departement.csv
        - type_logements_2014_par_departement.csv
        - naissances_2014_par_departement.csv
        - crimes_delits_2014_par_departement.csv
        - demandeurs_emplois_2014_par_departement.csv

    Colonnes de sortie :
        dep_code, dep_libelle, annee,
        revenu_fiscal_moyen_par_foyer, pct_foyers_imposes, ratio_actifs_retraites,
        nb_policiers_municipaux, naissances, nb_crimes_delits, nombre_demandeurs_emploi,
        pct_individuel_pur, pct_individuel_groupe, pct_collectif, pct_residence
    """
    print("\n" + "=" * 70)
    print("ðŸ¥‡ GOLD LAYER 2014 - Consolidation des donnÃ©es 2014")
    print("=" * 70)

    if output_file is None:
        output_file = 'data/gold/departements_2014.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # â”€â”€ 1. BASE : logements 2014 (fournit dep_code + dep_libelle) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nðŸ“ Construction de la base (type_logements 2014)...")
    lgt_file = 'data/silver/type_logements_2014_par_departement.csv'
    if not os.path.exists(lgt_file):
        print(f"âŒ Fichier introuvable: {lgt_file}")
        return None

    base = _read(lgt_file)
    base = base[base['annee'] == 2014].copy()
    base['dep_code'] = base['dep_code'].astype(str).str.strip()
    print(f"   â†’ {len(base)} lignes de base ({base['dep_code'].nunique()} depts)")

    # â”€â”€ 2. REVENUS FISCAUX 2014 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("\nðŸ”— Jointure revenus fiscaux 2014...")
    rev_file = 'data/silver/revenus_fiscaux_2014_par_departement.csv'
    if os.path.exists(rev_file):
        rev = _read(rev_file)
        rev['dep_code'] = rev['dep'].apply(_normalize_impots_dep)
        rev = rev[rev['dep_code'].notna()]
        base = base.merge(
            rev[['dep_code', 'pct_foyers_imposes',
                 'revenu_fiscal_moyen_par_foyer', 'ratio_actifs_retraites']],
            on='dep_code', how='left'
        )
    else:
        print(f"   âš ï¸  {rev_file} non trouvÃ© â€” colonnes laissÃ©es vides")
        for col in ('pct_foyers_imposes', 'revenu_fiscal_moyen_par_foyer', 'ratio_actifs_retraites'):
            base[col] = float('nan')

    # â”€â”€ 3. POLICIERS MUNICIPAUX 2014 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure policiers municipaux 2014...")
    pol_file = 'data/silver/polices_municipaux_2014_par_departement.csv'
    if os.path.exists(pol_file):
        police = _read(pol_file)
        police['dep_code'] = police['dep_code'].astype(str).str.strip()
        base = base.merge(
            police[['dep_code', 'nb_policiers_municipaux']],
            on='dep_code', how='left'
        )
    else:
        print(f"   âš ï¸  {pol_file} non trouvÃ© â€” colonne laissÃ©e vide")
        base['nb_policiers_municipaux'] = float('nan')

    # â”€â”€ 4. NAISSANCES 2014 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure naissances 2014...")
    nais_file = 'data/silver/naissances_2014_par_departement.csv'
    if os.path.exists(nais_file):
        nais = _read(nais_file)
        nais['dep_code'] = nais['dep_code'].astype(str).str.strip()
        base = base.merge(
            nais[['dep_code', 'naissances']],
            on='dep_code', how='left'
        )
    else:
        print(f"   âš ï¸  {nais_file} non trouvÃ© â€” colonne laissÃ©e vide")
        base['naissances'] = float('nan')

    # â”€â”€ 5. CRIMES ET DÃ‰LITS 2016 (proxy GN+PN, source data.gouv) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure crimes et dÃ©lits 2016 (GN+PN)...")
    cri_file = 'data/silver/crimes_delits_par_departement.csv'
    if os.path.exists(cri_file):
        cri = _read(cri_file)
        cri['code_departement'] = cri['code_departement'].astype(str).str.strip()
        cri16 = cri[cri['annee'] == 2016].copy()
        cri16 = cri16.rename(columns={
            'code_departement': 'dep_code',
            'taux_pour_mille': 'taux_crimes_pour_mille_2016',
        })
        base = base.merge(
            cri16[['dep_code', 'taux_crimes_pour_mille_2016']],
            on='dep_code', how='left'
        )
    else:
        print(f"   âš ï¸  {cri_file} non trouvÃ© â€” colonne laissÃ©e vide")
        base['taux_crimes_pour_mille_2016'] = float('nan')

    # â”€â”€ 6. DEMANDEURS Dâ€™EMPLOI 2014 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure demandeurs dâ€™emploi 2014...")
    dmd_file = 'data/silver/demandeurs_emplois_2014_par_departement.csv'
    if os.path.exists(dmd_file):
        dmd = _read(dmd_file)
        dmd['dep_code'] = dmd['dep_code'].astype(str).str.strip()
        base = base.merge(
            dmd[['dep_code', 'nombre_demandeurs_emploi']],
            on='dep_code', how='left'
        )
    else:
        print(f"   âš ï¸  {dmd_file} non trouvÃ© â€” colonne laissÃ©e vide")
        base['nombre_demandeurs_emploi'] = float('nan')

    # â”€â”€ 7. Ã‰LECTIONS 2014 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure Ã©lections 2014...")
    elec_file = 'data/silver/elections_2014_gagnants_par_departement.csv'
    if os.path.exists(elec_file):
        elec = _read(elec_file)
        elec['code_departement'] = elec['code_departement'].astype(str).str.strip()
        elec.rename(columns={
            'code_departement':       'dep_code',
            'libelle_abrege_liste':   'liste_gagnante',
            'bord_liste_gagnante':    'nuance_liste_gagnante',
            'pct_voix_exprimes':      'pct_voix_liste_gagnante',
            '% vote extreme gauche':  'pct_vote_extreme_gauche',
            '% vote gauche':          'pct_vote_gauche',
            '% vote centre':          'pct_vote_centre',
            '% vote droite':          'pct_vote_droite',
            '% vote extreme droite':  'pct_vote_extreme_droite',
        }, inplace=True)
        elec_cols = ['dep_code', 'nuance_liste_gagnante', 'pct_voix_liste_gagnante',
                     'pct_vote_extreme_gauche', 'pct_vote_gauche', 'pct_vote_centre',
                     'pct_vote_droite', 'pct_vote_extreme_droite']
        base = base.merge(
            elec[[c for c in elec_cols if c in elec.columns]],
            on='dep_code', how='left'
        )
        base['nuance_liste_gagnante'] = base['nuance_liste_gagnante'].fillna('NON COMMUNIQUE')
    else:
        print(f"   âš ï¸  {elec_file} non trouvÃ© â€” colonnes laissÃ©es vides")
        for col in ('nuance_liste_gagnante', 'pct_voix_liste_gagnante',
                    'pct_vote_extreme_gauche', 'pct_vote_gauche', 'pct_vote_centre',
                    'pct_vote_droite', 'pct_vote_extreme_droite'):
            base[col] = float('nan') if col != 'nuance_liste_gagnante' else 'NON COMMUNIQUE'

    # â”€â”€ 8. POPULATION PAR TRANCHE D'Ã‚GE 2014 â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ”— Jointure population tranche Ã¢ge 2014...")
    pop_file = 'data/silver/population_tranche_2014_par_departement.csv'
    if os.path.exists(pop_file):
        pop = _read(pop_file)
        pop['dep_code'] = pop['dep_code'].astype(str).str.strip()
        base = base.merge(
            pop[['dep_code', 'pop_total', 'pop_0_14', 'pop_15_29',
                 'pop_30_64', 'pop_65plus']],
            on='dep_code', how='left'
        )
    else:
        print(f"   âš ï¸  {pop_file} non trouvÃ© â€” colonnes laissÃ©es vides")
        for col in ('pop_total', 'pop_0_14', 'pop_15_29', 'pop_30_64', 'pop_65plus'):
            base[col] = float('nan')

    # â”€â”€ 8. CONVERSION EN POURCENTAGES (base : pop_total) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("ðŸ“ Conversion des tranches d'Ã¢ge en pourcentages (base: pop_total)...")
    for raw_col, pct_col in [
        ('pop_0_14',  'pct_pop_0_14'),
        ('pop_15_29', 'pct_pop_15_29'),
        ('pop_30_64', 'pct_pop_30_64'),
        ('pop_65plus','pct_pop_65plus'),
    ]:
        base[pct_col] = (base[raw_col] / base['pop_total'] * 100).round(2)
        base.drop(columns=[raw_col], inplace=True)

    print("ðŸ“ Conversion des effectifs en pourcentages / taux...")
    base['pct_policiers_municipaux'] = (base['nb_policiers_municipaux'] / base['pop_total'] * 100).round(4)
    base.drop(columns=['nb_policiers_municipaux'], inplace=True)

    base['pct_naissances'] = (base['naissances'] / base['pop_total'] * 100).round(2)
    base.drop(columns=['naissances'], inplace=True)

    base['pct_demandeurs_emploi'] = (base['nombre_demandeurs_emploi'] / base['pop_total'] * 100).round(2)
    base.drop(columns=['nombre_demandeurs_emploi'], inplace=True)

    # â”€â”€ 9. RÃ‰SUMÃ‰ â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    final_cols = [
        'dep_code', 'dep_libelle', 'annee',
        'pop_total', 'pct_pop_0_14', 'pct_pop_15_29', 'pct_pop_30_64', 'pct_pop_65plus',
        'revenu_fiscal_moyen_par_foyer', 'pct_foyers_imposes', 'ratio_actifs_retraites',
        'pct_policiers_municipaux', 'pct_naissances', 'taux_crimes_pour_mille_2016',
        'pct_demandeurs_emploi',
        'nuance_liste_gagnante', 'pct_voix_liste_gagnante',
        'pct_vote_extreme_gauche', 'pct_vote_gauche', 'pct_vote_centre',
        'pct_vote_droite', 'pct_vote_extreme_droite',
        'pct_individuel_pur', 'pct_individuel_groupe', 'pct_collectif', 'pct_residence',
    ]
    base = base[[c for c in final_cols if c in base.columns]].copy()

    print(f"\nðŸ“Š RÃ©sumÃ© ({len(base)} lignes Ã— {len(base.columns)} colonnes) :")
    data_cols = [c for c in base.columns if c not in ('dep_code', 'dep_libelle', 'annee')]
    for col in data_cols:
        n_null = base[col].isna().sum()
        coverage = round((1 - n_null / len(base)) * 100, 1)
        flag = "âœ…" if n_null == 0 else ("âš ï¸ " if n_null <= 10 else "âŒ")
        print(f"   {flag} {col:<45} {coverage:>5}% ({n_null} NaN)")

    print(f"\nðŸ’¾ Sauvegarde vers: {output_file}")
    base.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    print(f"   â†’ {len(base)} lignes, {len(base.columns)} colonnes")

    print("\nâœ… Gold Layer 2014 terminÃ©")
    return base


if __name__ == "__main__":
    process_gold_layer()
    process_gold_2014_layer()

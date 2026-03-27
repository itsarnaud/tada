"""
Script de traitement des effectifs de police municipale par département
Sources : effectifs-police-municipale-2019-.ods  /  enquete-stats-pm-2024.ods
Sortie   : une ligne par département par année (2019 & 2024)
Colonnes : dep_libelle, dep_code, annee, pct_policiers_municipaux
"""

import os
import pandas as pd


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_dep_code(x) -> bool:
    """Retourne True si x est un code département valide (entier ou 2A/2B)."""
    if pd.isna(x):
        return False
    x_str = str(x).strip()
    if x_str.upper() in ('2A', '2B'):
        return True
    try:
        int(float(x_str))
        return True
    except ValueError:
        return False


def _fmt_dep_code(x) -> str | None:
    """Formate un code département : 1 -> '01', 971 -> '971', '2A' -> '2A'."""
    if pd.isna(x):
        return None
    x_str = str(x).strip()
    if x_str.upper() in ('2A', '2B'):
        return x_str.upper()
    try:
        n = int(float(x_str))
        return f'{n:02d}' if n < 100 else str(n)
    except ValueError:
        return x_str


def _extract_year(filepath: str, agents_col: int, year: int) -> pd.DataFrame:
    """
    Charge un fichier ODS et extrait les totaux par département.

    Stratégie :
      - Les lignes « TOTAL XXX » (col0) contiennent déjà le total agrégé.
      - On forward-fill le dep_code (col0 des lignes communes) et le
        dep_libelle (col1 pour 2019, col0 pour 2024) jusqu'aux lignes TOTAL.
    """
    df = pd.read_excel(filepath, engine='odf', header=None)
    # Uniformiser les noms de colonnes
    df.columns = range(len(df.columns))

    # ── Masques ──────────────────────────────────────────────────────────────
    # Lignes TOTAL classiques (hors TOTAL NATIONAL)
    total_mask_prefix = (
        df[0].astype(str).str.match(r'^TOTAL [A-ZÀ-ÜÀ-ÿ]', na=False)
        & (df[0].astype(str).str.strip() != 'TOTAL NATIONAL')
    )

    # Certains depts en 2024 n'ont pas de préfixe TOTAL (ex : CHARENTE-MARITIME)
    # On les détecte via : col1=NaN, col2=NaN, col3 et col4 numériques non nuls
    def _is_positive_num(x) -> bool:
        try:
            return pd.notna(x) and float(x) > 0
        except (TypeError, ValueError):
            return False

    total_mask_no_prefix = (
        df[1].isna()
        & df[2].isna()
        & df[0].notna()
        & ~df[0].astype(str).str.startswith('TOTAL')
        & ~df[0].apply(_is_dep_code)
        & df[3].apply(_is_positive_num)
        & df[4].apply(_is_positive_num)
    )

    total_mask = total_mask_prefix | (total_mask_no_prefix if year == 2024 else pd.Series(False, index=df.index))

    # Lignes communes : col1 est NaN, col0 est un code département valide
    commune_mask = df[1].isna() & df[0].notna() & ~total_mask & df[0].apply(_is_dep_code)

    df = df.copy()

    # ── Forward-fill du code département ─────────────────────────────────────
    df['dep_code_raw'] = df[0].where(commune_mask)
    df['dep_code_raw'] = df['dep_code_raw'].ffill()

    # ── Forward-fill du libellé département ──────────────────────────────────
    if year == 2019:
        # Lignes header département : col0=NaN, col1=nom, col2=NaN
        hdr_mask = df[0].isna() & df[1].notna() & df[2].isna()
        df['dep_name_src'] = df[1].where(hdr_mask)
    else:
        # 2024 : le nom du département est dans col0 (en majuscules, parfois avec *)
        # Ces lignes ont : col1=NaN, col2=NaN, col0 n'est pas un code num/2A/2B, pas un TOTAL
        _title_kw = ('MI/SG', 'ENQUÊTE', 'LISTE', 'DÉPARTEMENT', 'NUMÉRO')

        def _is_dep_header_2024(x) -> bool:
            if not isinstance(x, str):
                return False
            x = x.strip()
            if not x or x.startswith('TOTAL'):
                return False
            if any(kw in x.upper() for kw in _title_kw):
                return False
            if _is_dep_code(x):
                return False
            return True

        hdr_mask = df[0].apply(_is_dep_header_2024) & df[1].isna() & df[2].isna() & ~total_mask_no_prefix
        # Nettoyer : supprimer *, mettre en title case
        df['dep_name_src'] = (
            df[0]
            .where(hdr_mask)
            .astype(str)
            .str.replace('*', '', regex=False)
            .str.strip()
            .str.title()
            .where(hdr_mask)   # remet NaN là où le masque est False
        )

    df['dep_name'] = df['dep_name_src'].ffill()

    # ── Extraction des lignes TOTAL ──────────────────────────────────────────
    result = df[total_mask][['dep_name', 'dep_code_raw', agents_col]].copy()
    result.columns = ['dep_libelle', 'dep_code_raw', 'nb_policiers_municipaux']
    result['nb_policiers_municipaux'] = (
        pd.to_numeric(result['nb_policiers_municipaux'], errors='coerce')
        .fillna(0)
        .astype(int)
    )
    result['annee'] = year
    result['dep_code'] = result['dep_code_raw'].apply(_fmt_dep_code)

    # Title case pour 2019 (déjà fait pour 2024 via dep_name_src)
    if year == 2019:
        result['dep_libelle'] = result['dep_libelle'].str.strip().str.title()
    # Pour les lignes sans préfixe TOTAL (ex: CHARENTE-MARITIME),
    # dep_name est forward-fill depuis le header, déjà en title case

    return result[['dep_libelle', 'dep_code', 'annee', 'nb_policiers_municipaux']].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def process_polices_municipaux_data(
    input_2019: str = None,
    input_2024: str = None,
    output_file: str = None,
) -> pd.DataFrame | None:
    """
    Traite les deux fichiers ODS (2019 et 2024) et produit un CSV consolidé.

    Colonnes de sortie :
        dep_libelle | dep_code | annee | pct_policiers_municipaux

    Territoires exclus : SPM, Collectivité de Saint-Martin, Collectivité de Saint-Barthélemy.
    Seuls les départements disposant de données de population (tranche_age) sont conservés.
    Le pourcentage = nb_policiers_municipaux / population_totale_département.

    Args:
        input_2019  : Chemin du fichier ODS 2019 (optionnel)
        input_2024  : Chemin du fichier ODS 2024 (optionnel)
        output_file : Chemin du fichier CSV de sortie (optionnel)
    """
    print("=" * 70)
    print("👮 Traitement des effectifs de police municipale (2019 & 2024)")
    print("=" * 70)

    if input_2019 is None:
        input_2019 = 'data/raw/POLICIERS MUNICIPAUX/effectifs-police-municipale-2019-.ods'
    if input_2024 is None:
        input_2024 = 'data/raw/POLICIERS MUNICIPAUX/enquete-stats-pm-2024.ods'
    if output_file is None:
        output_file = 'data/silver/polices_municipaux_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # ── Territoires à exclure ─────────────────────────────────────────────────
    EXCLUDED = {'spm', 'collectivité de saint-martin', 'collectivité de saint-barthélemy'}

    frames = []

    for filepath, agents_col, year in [
        (input_2019, 5, 2019),
        (input_2024, 6, 2024),
    ]:
        if not os.path.exists(filepath):
            print(f"⚠️  Fichier non trouvé: {filepath}")
            continue
        print(f"\n📥 Chargement ({year}): {filepath}")
        df_year = _extract_year(filepath, agents_col, year)
        print(f"   → {len(df_year)} départements extraits avant filtrage")
        frames.append(df_year)

    if not frames:
        return None

    df_all = pd.concat(frames, ignore_index=True)

    # ── Suppression des territoires exclus ───────────────────────────────────
    df_all = df_all[
        ~df_all['dep_libelle'].str.lower().str.strip().isin(EXCLUDED)
    ].copy()

    # ── Chargement de la population par département ───────────────────────────
    print("\n📊 Chargement des données de population (tranche_age)...")
    pop_frames = []
    for year, trage_file in [
        (2019, 'data/silver/tranche_age_2019_par_departement.csv'),
        (2024, 'data/silver/tranche_age_2024_par_departement.csv'),
    ]:
        if not os.path.exists(trage_file):
            print(f"⚠️  Fichier population non trouvé: {trage_file}")
            continue
        df_pop = pd.read_csv(trage_file, sep=',', encoding='utf-8-sig')
        df_pop['pop_total'] = (
            df_pop['pop_0_14'] + df_pop['pop_15_29'] +
            df_pop['pop_30_64'] + df_pop['pop_64plus']
        )
        df_pop_dep = df_pop.groupby('dep')['pop_total'].sum().reset_index()
        df_pop_dep['annee'] = year
        pop_frames.append(df_pop_dep)

    if not pop_frames:
        print("❌ Données de population indisponibles, calcul du % impossible")
        return None

    df_pop_all = pd.concat(pop_frames, ignore_index=True)
    df_pop_all.rename(columns={'dep': 'dep_code'}, inplace=True)

    # ── Jointure et calcul du pourcentage ────────────────────────────────────
    df_final = df_all.merge(df_pop_all, on=['dep_code', 'annee'], how='inner')
    df_final['pct_policiers_municipaux'] = (
        df_final['nb_policiers_municipaux'] / df_final['pop_total'] * 100
    ).round(4)

    df_final = df_final[
        ['dep_libelle', 'dep_code', 'annee', 'pct_policiers_municipaux']
    ].sort_values(['dep_code', 'annee']).reset_index(drop=True)

    print(f"\n💾 Sauvegarde vers: {output_file}")
    df_final.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    print(f"   → {len(df_final)} lignes exportées")

    print("\n📊 Aperçu des premières lignes:")
    print(df_final.head(10).to_string(index=False))

    print("\n✅ Traitement des effectifs de police municipale terminé")
    return df_final


if __name__ == "__main__":
    process_polices_municipaux_data()

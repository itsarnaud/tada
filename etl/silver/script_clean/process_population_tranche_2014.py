"""
Script de traitement des populations par tranche d'âge pour 2014.
Lit le fichier XLS IRIS RP2014 (infra-communal) de l'INSEE et agrège
par département.

Structure du fichier :
    - Feuille : 'IRIS', 50 100 lignes × 84 colonnes
    - Ligne 0-3 : métadonnées
    - Ligne 4 : libellés longs (Région, Département…)
    - Ligne 5 : codes courts  (REG, DEP, LIBCOM, P14_POP…)  ← utilisé comme header
    - Lignes 6+ : données

Colonnes de sortie :
    dep_code, annee, pop_total,
    pop_0_14, pop_15_29, pop_65plus, pop_30_64  (calculé : total - 0_14 - 15_29 - 65+)

Note : le fichier XLS est volumineux (~66 Mo) — le chargement peut prendre
plusieurs minutes.
"""

import os
import pandas as pd

# ---------------------------------------------------------------------------
# Mapping court-code → clé sémantique (fichier IRIS RP2014)
# ---------------------------------------------------------------------------
_COL_CANDIDATES = {
    'dep':       ['DEP',       'Département',  'Départements'],
    'pop_total': ['P14_POP',   'Population en 2014'],
    'pop_0_14':  ['P14_POP0014', 'Pop 0-14'],
    'pop_15_29': ['P14_POP1529', 'Pop 15-29'],
    'pop_65plus':['P14_POP65P',  'Pop 65 ans ou plus'],
}

_DEFAULT_INPUT  = 'data/raw/DATA 2014/POPULATION TRANCHE/population_tranche_age.xls'
_DEFAULT_OUTPUT = 'data/silver/population_tranche_2014_par_departement.csv'

_SHEET   = 'IRIS'
_SKIPROWS = 5   # skip lignes 0-4, ligne 5 = header codes courts


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_col(df: pd.DataFrame, key: str) -> str | None:
    """
    Retourne le nom réel de colonne correspondant à la clé sémantique.
    Essaie l'égalité exacte (insensible à la casse) puis la recherche partielle.
    """
    candidates = _COL_CANDIDATES[key]
    col_map = {str(c).strip(): c for c in df.columns}   # cleaned → original

    for candidate in candidates:
        # 1. Exact match (case-insensitive)
        for clean, orig in col_map.items():
            if clean.upper() == candidate.upper():
                return orig
        # 2. Partial match fallback
        for clean, orig in col_map.items():
            if candidate.lower() in clean.lower():
                return orig
    return None


# ---------------------------------------------------------------------------
# Fonction principale
# ---------------------------------------------------------------------------

def process_population_tranche_2014_data(
    input_file: str = None,
    output_file: str = None,
) -> pd.DataFrame | None:
    """
    Charge le fichier XLS IRIS RP2014 et produit un CSV agrégé par département
    avec 4 tranches d'âge (pop_0_14, pop_15_29, pop_65plus, pop_30_64 calculé).
    """
    print("=" * 70)
    print("👥 Traitement des tranches d'âge 2014 (IRIS RP2014)")
    print("=" * 70)

    if input_file is None:
        input_file = _DEFAULT_INPUT
    if output_file is None:
        output_file = _DEFAULT_OUTPUT

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé : {input_file}")
        return None

    file_mb = os.path.getsize(input_file) / (1024 * 1024)
    print(f"\n📥 Chargement du fichier : {input_file}  ({file_mb:.1f} Mo)")
    print(f"   Feuille : '{_SHEET}', skiprows={_SKIPROWS}")
    print("   ⏳ Chargement en cours (fichier volumineux, peut prendre plusieurs minutes)…")

    # ── Chargement complet ────────────────────────────────────────────────────
    df = pd.read_excel(
        input_file,
        sheet_name=_SHEET,
        skiprows=_SKIPROWS,
        engine='xlrd',
        dtype=str,
    )
    # Nettoyer les noms de colonnes (espaces parasites)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"   → {len(df)} lignes, {len(df.columns)} colonnes chargées")

    # ── Identifier les colonnes cibles ────────────────────────────────────────
    col_dep      = _find_col(df, 'dep')
    col_pop_tot  = _find_col(df, 'pop_total')
    col_0_14     = _find_col(df, 'pop_0_14')
    col_15_29    = _find_col(df, 'pop_15_29')
    col_65plus   = _find_col(df, 'pop_65plus')

    _map = {
        'dep_col':     col_dep,
        'pop_total':   col_pop_tot,
        'pop_0_14':    col_0_14,
        'pop_15_29':   col_15_29,
        'pop_65plus':  col_65plus,
    }
    missing = [k for k, v in _map.items() if v is None]
    if missing:
        print(f"\n❌ Colonnes introuvables : {missing}")
        print("   Colonnes disponibles :")
        for c in df.columns[:30]:
            print(f"     {repr(c)}")
        return None

    print(f"\n✅ Colonnes identifiées :")
    for k, v in _map.items():
        print(f"   {k:<15} → {repr(v)}")

    # ── Sélection et nettoyage ────────────────────────────────────────────────
    cols_to_keep = list(_map.values())
    df = df[cols_to_keep].copy()
    df.columns = list(_map.keys())

    # Garder uniquement les lignes avec un code département valide (01-95, 2A, 2B, 971-976)
    df['dep_code'] = df['dep_col'].astype(str).str.strip().str.zfill(2)
    df = df[df['dep_code'].str.match(
        r'^(0[1-9]|[1-8]\d|9[0-5]|2[AB]|97[1-6])$'
    )].copy()
    print(f"\n   → {len(df)} lignes IRIS avec code département valide")

    # Convertir les colonnes numériques
    for col in ['pop_total', 'pop_0_14', 'pop_15_29', 'pop_65plus']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # ── Agrégation par département ────────────────────────────────────────────
    print("\n📊 Agrégation par département…")
    agg = df.groupby('dep_code')[
        ['pop_total', 'pop_0_14', 'pop_15_29', 'pop_65plus']
    ].sum().reset_index()

    agg['annee']    = 2014
    agg['pop_total']  = agg['pop_total'].round().astype(int)
    agg['pop_0_14']   = agg['pop_0_14'].round().astype(int)
    agg['pop_15_29']  = agg['pop_15_29'].round().astype(int)
    agg['pop_65plus'] = agg['pop_65plus'].round().astype(int)
    agg['pop_30_64']  = agg['pop_total'] - agg['pop_0_14'] - agg['pop_15_29'] - agg['pop_65plus']

    df_final = agg[['dep_code', 'annee', 'pop_total',
                    'pop_0_14', 'pop_15_29', 'pop_30_64', 'pop_65plus']]

    df_final = df_final.sort_values('dep_code').reset_index(drop=True)

    # ── Sauvegarde ────────────────────────────────────────────────────────────
    print(f"\n💾 Sauvegarde : {output_file}")
    df_final.to_csv(output_file, index=False, encoding='utf-8-sig')

    print(f"\n✅ Traitement terminé !")
    print(f"   → Fichier créé : {output_file}")
    print(f"   → {len(df_final)} départements")
    print(f"   → Colonnes : {', '.join(df_final.columns)}")
    print("\n📊 Aperçu :")
    print(df_final.head(10).to_string(index=False))
    print("\n" + "=" * 70)

    return df_final


if __name__ == "__main__":
    process_population_tranche_2014_data()

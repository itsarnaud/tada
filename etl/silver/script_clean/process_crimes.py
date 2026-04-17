"""
Process crimes et delits data.
Aggregates by department and year (2019, 2024).
Outputs: code_departement, taux_pour_mille (moyenne de tous les indicateurs), annee
"""

import pandas as pd
from pathlib import Path

def process_crimes():
    """Process crimes and delits data by averaging all indicators per department and year"""
    
    # Paths
    raw_path = Path("data/raw/CRIMES ET DELITS")
    bronze_path = Path("data/silver")
    bronze_path.mkdir(parents=True, exist_ok=True)
    
    # Input file
    input_file = raw_path / "donnee-dep-data.gouv-2025-geographie2025-produit-le2026-01-22.csv"
    
    # Load data
    print(f"Loading {input_file}...")
    df = pd.read_csv(input_file, sep=';')
    
    print(f"Loaded {len(df)} rows")
    print(f"Années disponibles: {sorted(df['annee'].unique())}")
    
    # Filter for years 2019 and 2024 only
    df_filtered = df[df['annee'].isin([2019, 2024])].copy()
    print(f"Filtered to {len(df_filtered)} rows for years 2019 and 2024")
    
    # Convert taux_pour_mille to numeric (replace comma with dot for French decimal format)
    df_filtered['taux_pour_mille'] = df_filtered['taux_pour_mille'].str.replace(',', '.').astype(float)
    
    # Group by department and year, calculate mean of taux_pour_mille across all indicators
    df_agg = df_filtered.groupby(['Code_departement', 'annee'], as_index=False).agg({
        'taux_pour_mille': 'mean'
    })
    
    # Rename columns
    df_agg = df_agg.rename(columns={
        'Code_departement': 'code_departement'
    })
    
    # Round taux_pour_mille to 2 decimals
    df_agg['taux_pour_mille'] = df_agg['taux_pour_mille'].round(2)
    
    # Sort by department and year
    df_agg = df_agg.sort_values(['code_departement', 'annee']).reset_index(drop=True)
    
    # Output file
    output_file = bronze_path / "crimes_delits_par_departement.csv"
    df_agg.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\nSaved to {output_file}")
    print(f"Total rows: {len(df_agg)} (2 per department)")
    print(f"Departments: {df_agg['code_departement'].nunique()}")
    print("\nSample data:")
    print(df_agg.head(10))
    print("\n...")
    print(df_agg.tail(5))

    return df_agg


def process_crimes_2014_data(input_file=None, output_file=None):
    """
    Traite la feuille 'Services GN 2014' du fichier XLSX crimes/délits.

    Structure de la feuille :
        Ligne 0 : code département répété pour chaque CGD (colonne ≥ 2)
        Ligne 1 : noms des CGD (ignoré)
        Lignes 2+ : types de crimes/délits — col 0 = index, col 1 = libellé, col 2+ = valeurs

    Traitement :
        - Exclure les lignes "Index non utilisé"
        - Pour chaque colonne, associer le code département (ligne 0)
        - Sommer toutes les valeurs (tous types × tous CGD) par département
        - Sortie : dep_code, annee, nb_crimes_delits

    Args:
        input_file:  Chemin du XLSX source (optionnel)
        output_file: Chemin du CSV de sortie (optionnel)
    """
    import os

    print("=" * 70)
    print("🚨 Traitement des crimes et délits GN 2014")
    print("=" * 70)

    if input_file is None:
        input_file = (
            'data/raw/DATA 2014/CRIME/'
            'crimes-et-delits-enregistres-par-les-services-de-gendarmerie-'
            'et-de-police-depuis-2012.xlsx'
        )
    if output_file is None:
        output_file = 'data/silver/crimes_delits_2014_par_departement.csv'

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    if not os.path.exists(input_file):
        print(f"⚠️  Fichier non trouvé: {input_file}")
        return None

    print(f"\n📥 Chargement de la feuille 'Services GN 2014': {input_file}")
    df_raw = pd.read_excel(input_file, sheet_name='Services GN 2014', header=None)
    print(f"   → {df_raw.shape[0]} lignes × {df_raw.shape[1]} colonnes")

    # ── Récupérer les codes département (ligne 0, à partir de la col 2) ──────
    dep_row = df_raw.iloc[0, 2:].tolist()   # ex: [01, 01, 01, 02, 02, ...]

    # ── Données : lignes 2+ (sauter ligne 0 = deps, ligne 1 = noms CGD) ──────
    df_data = df_raw.iloc[2:].copy().reset_index(drop=True)

    # Exclure les lignes "Index non utilisé"
    libelle_col = df_data.iloc[:, 1].astype(str).str.strip()
    mask_valid = ~libelle_col.str.lower().str.contains('non utilisé|non utilise', na=False)
    df_data = df_data[mask_valid].copy()
    print(f"   → {len(df_data)} types de crimes/délits valides (sur {df_raw.shape[0]-2})")

    # ── Construire un dict dep_code → somme totale ────────────────────────────
    records: dict[str, int] = {}
    for col_offset, dep_raw in enumerate(dep_row):
        col_idx = col_offset + 2  # décalage : cols 0 et 1 sont index/libellé
        if pd.isna(dep_raw):
            continue
        # Normaliser : 75.0 → '75', '01' → '01', 971 → '971'
        try:
            dep_int = int(float(str(dep_raw).strip()))
            dep_code = f'{dep_int:02d}' if dep_int < 100 else str(dep_int)
        except (ValueError, TypeError):
            dep_code = str(dep_raw).strip()
        if not dep_code or len(dep_code) > 3:
            continue  # valeur parasite

        col_vals = pd.to_numeric(df_data.iloc[:, col_idx], errors='coerce').fillna(0)
        records[dep_code] = records.get(dep_code, 0) + int(col_vals.sum())

    df_out = pd.DataFrame([
        {'dep_code': dep, 'annee': 2014, 'nb_crimes_delits': cnt}
        for dep, cnt in records.items()
    ]).sort_values('dep_code').reset_index(drop=True)

    print(f"\n💾 Sauvegarde vers: {output_file}")
    df_out.to_csv(output_file, sep=';', index=False, encoding='utf-8-sig')
    print(f"   → {len(df_out)} départements exportés")

    print("\n📊 Aperçu des premières lignes:")
    print(df_out.head(10).to_string(index=False))

    print("\n✅ Traitement crimes/délits GN 2014 terminé")
    return df_out

if __name__ == "__main__":
    process_crimes()
    process_crimes_2014_data()

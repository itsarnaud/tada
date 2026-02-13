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
    bronze_path = Path("data/bronze")
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

if __name__ == "__main__":
    process_crimes()

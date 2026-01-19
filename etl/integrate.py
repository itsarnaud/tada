"""
Script d'intégration des données pour créer le dataset final
Fusionne toutes les sources de données nettoyées par commune et exporte en CSV
"""

import pandas as pd
import os
from datetime import datetime

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_PATH = r'C:\Users\theop\Desktop\data\clean'
DESKTOP_PATH = os.path.join(os.path.expanduser('~'), 'Desktop')

os.makedirs(PROCESSED_PATH, exist_ok=True)


def load_processed_data():
    """
    Charge toutes les données nettoyées
    """
    print("📂 Chargement des données traitées...")
    
    data = {}
    files = {
        'elections': 'elections_2020_clean.csv',
        'population': 'population_age_clean.csv',
        'logements': 'logements_clean.csv',
        'pauvrete': 'pauvrete_clean.csv',
        'education': 'education_clean.csv',
        'emploi': 'emploi_clean.csv',
        'sociopro': 'sociopro_clean.csv',
        'communes': 'communes_herault_clean.csv'
    }
    
    for name, filename in files.items():
        filepath = os.path.join(PROCESSED_PATH, filename)
        if os.path.exists(filepath):
            data[name] = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
            print(f"  ✓ {name}: {len(data[name])} lignes")
        else:
            print(f"  ⚠️  {name}: fichier non trouvé")
    
    return data


def identify_commune_columns(data):
    """
    Identifie les colonnes communes dans chaque dataset pour la fusion
    """
    print("\n🔍 Identification des colonnes de jointure...")
    
    for name, df in data.items():
        print(f"\n{name}:")
        print(f"  Colonnes: {', '.join(df.columns[:10])}")
        
        # Chercher les colonnes potentielles de commune
        commune_cols = [col for col in df.columns if any(
            keyword in col.lower() 
            for keyword in ['commune', 'ville', 'libelle', 'nom', 'code_commune', 'insee']
        )]
        if commune_cols:
            print(f"  Colonnes communes identifiées: {', '.join(commune_cols)}")


def find_common_column(data):
    """
    Trouve la colonne commune pour les jointures (code INSEE, nom commune, etc.)
    """
    print("\n🔍 Recherche de la colonne de jointure commune...")
    
    # Chercher la colonne commune dans tous les datasets
    all_columns = {}
    for name, df in data.items():
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['insee', 'code_commune', 'code_ins']):
                if col not in all_columns:
                    all_columns[col] = []
                all_columns[col].append(name)
    
    # Trouver la colonne présente dans le plus de datasets
    if all_columns:
        best_col = max(all_columns.items(), key=lambda x: len(x[1]))
        print(f"  ✓ Colonne de jointure trouvée: '{best_col[0]}' (présente dans {len(best_col[1])} datasets)")
        return best_col[0]
    
    # Alternative : chercher nom de commune
    for name, df in data.items():
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['commune', 'libelle', 'nom_commune']):
                print(f"  ✓ Colonne de jointure trouvée: '{col}'")
                return col
    
    print("  ⚠️ Aucune colonne de jointure évidente trouvée")
    return None


def merge_all_data(data, join_column=None):
    """
    Fusionne tous les datasets en un seul DataFrame
    """
    print("\n🔗 Fusion des données...")
    
    if not data:
        return None
    
    # Commencer avec le premier dataset (communes ou le plus complet)
    if 'communes' in data:
        master_df = data['communes'].copy()
        print(f"  ✓ Base: communes ({len(master_df)} lignes)")
    else:
        # Prendre le premier dataset disponible
        first_key = list(data.keys())[0]
        master_df = data[first_key].copy()
        print(f"  ✓ Base: {first_key} ({len(master_df)} lignes)")
    
    # Si on n'a pas trouvé de colonne de jointure, essayer automatiquement
    if not join_column:
        # Chercher une colonne commune dans master_df
        for col in master_df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['insee', 'code_commune', 'code_ins', 'commune', 'libelle']):
                join_column = col
                break
    
    if not join_column:
        print("  ⚠️ Impossible de trouver une colonne de jointure")
        return master_df
    
    print(f"  → Utilisation de la colonne: '{join_column}'")
    
    # Fusionner les autres datasets
    datasets_to_merge = [name for name in data.keys() if name != 'communes' and name != list(data.keys())[0]]
    
    for name in datasets_to_merge:
        df = data[name]
        
        # Chercher la colonne correspondante dans ce dataset
        matching_col = None
        for col in df.columns:
            if col == join_column or col.lower() == join_column.lower():
                matching_col = col
                break
        
        if matching_col:
            # Fusionner
            before_count = len(master_df)
            master_df = master_df.merge(df, left_on=join_column, right_on=matching_col, 
                                       how='left', suffixes=('', f'_{name}'))
            print(f"  ✓ Fusion avec {name}: {len(df)} lignes → {len(master_df)} lignes finales")
        else:
            print(f"  ⚠️ Impossible de fusionner {name} (colonne de jointure non trouvée)")
    
    return master_df


def export_to_desktop_csv(df, filename='dataset_final_electio_analytics.csv'):
    """
    Exporte le dataset final en CSV sur le bureau
    """
    print(f"\n💾 Export du dataset final...")
    
    csv_path = os.path.join(DESKTOP_PATH, filename)
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    print(f"  ✓ Dataset sauvegardé: {csv_path}")
    print(f"  ✓ Nombre de lignes: {len(df)}")
    print(f"  ✓ Nombre de colonnes: {len(df.columns)}")
    
    return csv_path


def create_data_dictionary(data):
    """
    Crée un dictionnaire de données documentant chaque table et colonne
    """
    print("\n📖 Création du dictionnaire de données...")
    
    dict_path = os.path.join(DESKTOP_PATH, 'data_dictionary_electio.csv')
    
    rows = []
    for table_name, df in data.items():
        for col in df.columns:
            rows.append({
                'table': table_name,
                'colonne': col,
                'type': str(df[col].dtype),
                'non_null': df[col].notna().sum(),
                'null': df[col].isna().sum(),
                'unique_values': df[col].nunique(),
                'exemple': str(df[col].iloc[0]) if len(df) > 0 else ''
            })
    
    dict_df = pd.DataFrame(rows)
    dict_df.to_csv(dict_path, index=False, encoding='utf-8-sig')
    
    print(f"  ✓ Dictionnaire sauvegardé: {dict_path}")
    return dict_path


def main():
    """
    Fonction principale d'intégration
    """
    print("=" * 70)
    print("🔄 INTÉGRATION DES DONNÉES - ELECTIO-ANALYTICS")
    print("=" * 70)
    
    # Charger les données
    data = load_processed_data()
    
    if not data:
        print("\n❌ Aucune donnée à traiter. Exécutez d'abord etl/main.py")
        return
    
    # Analyser la structure
    identify_commune_columns(data)
    
    # Trouver la colonne de jointure
    join_column = find_common_column(data)
    
    # Fusionner toutes les données
    master_df = merge_all_data(data, join_column)
    
    if master_df is None or master_df.empty:
        print("\n❌ Impossible de créer le dataset fusionné")
        return
    
    # Créer le dictionnaire de données
    dict_path = create_data_dictionary(data)
    
    # Exporter en CSV sur le bureau
    csv_path = export_to_desktop_csv(master_df)
    
    print("\n" + "=" * 70)
    print("✅ INTÉGRATION TERMINÉE")
    print("=" * 70)
    print(f"\n📊 Dataset final CSV: {csv_path}")
    print(f"📖 Dictionnaire: {dict_path}")
    print(f"\n💡 Le fichier CSV est prêt sur votre bureau !")


if __name__ == "__main__":
    main()

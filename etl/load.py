"""
Module de chargement des données pour le projet Electio-Analytics
Gère l'écriture des données transformées dans différents formats
"""

from pyspark.sql import DataFrame
import os


def save_to_csv(df: DataFrame, path: str, mode='overwrite', header=True):
    """
    Sauvegarde le DataFrame en CSV
    
    Args:
        df: DataFrame à sauvegarder
        path: chemin de destination
        mode: 'overwrite', 'append', 'ignore', 'error'
        header: inclure les en-têtes
    """
    df.coalesce(1).write.mode(mode).option('header', header).csv(path)
    print(f"✓ Données sauvegardées en CSV: {path}")


def save_to_parquet(df: DataFrame, path: str, mode='overwrite', partition_by=None):
    """
    Sauvegarde le DataFrame en Parquet (format optimisé)
    
    Args:
        df: DataFrame à sauvegarder
        path: chemin de destination
        mode: mode d'écriture
        partition_by: colonnes de partitionnement
    """
    if partition_by:
        df.write.mode(mode).partitionBy(partition_by).parquet(path)
    else:
        df.write.mode(mode).parquet(path)
    print(f"✓ Données sauvegardées en Parquet: {path}")


def save_to_json(df: DataFrame, path: str, mode='overwrite'):
    """
    Sauvegarde le DataFrame en JSON
    """
    df.coalesce(1).write.mode(mode).json(path)
    print(f"✓ Données sauvegardées en JSON: {path}")


def export_to_sqlite(df: DataFrame, db_path: str, table_name: str, mode='overwrite'):
    """
    Exporte les données vers une base SQLite
    
    Args:
        df: DataFrame à exporter
        db_path: chemin de la base SQLite
        table_name: nom de la table
        mode: 'overwrite' ou 'append'
    """
    df.write \
        .format('jdbc') \
        .option('url', f'jdbc:sqlite:{db_path}') \
        .option('dbtable', table_name) \
        .mode(mode) \
        .save()
    print(f"✓ Données exportées vers SQLite: {db_path} (table: {table_name})")


def save_pandas_to_csv(df_pandas, path: str, index=False):
    """
    Sauvegarde un DataFrame Pandas en CSV
    
    Args:
        df_pandas: DataFrame pandas
        path: chemin de destination
        index: inclure l'index
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df_pandas.to_csv(path, index=index, encoding='utf-8')
    print(f"✓ Données sauvegardées en CSV (pandas): {path}")


def create_summary_report(df: DataFrame, output_path: str):
    """
    Crée un rapport de synthèse des données
    
    Args:
        df: DataFrame à analyser
        output_path: chemin du fichier de rapport
    """
    summary = {
        'nombre_lignes': df.count(),
        'nombre_colonnes': len(df.columns),
        'colonnes': df.columns,
        'types': {field.name: str(field.dataType) for field in df.schema.fields},
        'statistiques': df.describe().toPandas().to_dict()
    }
    
    import json
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Rapport de synthèse créé: {output_path}")
    return summary

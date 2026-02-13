"""
Module de chargement des données
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
    
    Returns:
        None
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.coalesce(1).write.mode(mode).option('header', header).csv(path)
    print(f"✓ Données sauvegardées en CSV: {path}")
    print(f"  → {df.count()} lignes sauvegardées")


def save_to_parquet(df: DataFrame, path: str, mode='overwrite', partition_by=None):
    """
    Sauvegarde le DataFrame en Parquet (format optimisé)
    
    Args:
        df: DataFrame à sauvegarder
        path: chemin de destination
        mode: mode d'écriture
        partition_by: colonnes de partitionnement (liste)
    
    Returns:
        None
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    if partition_by:
        df.write.mode(mode).partitionBy(partition_by).parquet(path)
    else:
        df.write.mode(mode).parquet(path)
    
    print(f"✓ Données sauvegardées en Parquet: {path}")
    print(f"  → {df.count()} lignes sauvegardées")


def save_to_json(df: DataFrame, path: str, mode='overwrite'):
    """
    Sauvegarde le DataFrame en JSON
    
    Args:
        df: DataFrame à sauvegarder
        path: chemin de destination
        mode: mode d'écriture
    
    Returns:
        None
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.coalesce(1).write.mode(mode).json(path)
    print(f"✓ Données sauvegardées en JSON: {path}")


def save_multiple_formats(df: DataFrame, base_path: str, formats=['csv', 'parquet']):
    """
    Sauvegarde le DataFrame dans plusieurs formats
    
    Args:
        df: DataFrame à sauvegarder
        base_path: chemin de base (sans extension)
        formats: liste des formats souhaités
    
    Returns:
        None
    """
    for fmt in formats:
        if fmt == 'csv':
            save_to_csv(df, f"{base_path}.csv")
        elif fmt == 'parquet':
            save_to_parquet(df, f"{base_path}.parquet")
        elif fmt == 'json':
            save_to_json(df, f"{base_path}.json")
    
    print(f"✓ Données sauvegardées dans {len(formats)} formats")

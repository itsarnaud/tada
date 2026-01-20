"""
Module de transformation des données pour le projet Electio-Analytics
Nettoie, normalise et prépare les données pour l'analyse et la modélisation
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, when, regexp_replace, lower, lit
from pyspark.sql.types import IntegerType, DoubleType, StringType
import re


def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Nettoie les noms de colonnes : supprime espaces, caractères spéciaux, 
    met en minuscule et remplace espaces par underscores
    """
    for col_name in df.columns:
        new_col_name = col_name.lower()
        new_col_name = re.sub(r'[éèêë]', 'e', new_col_name)
        new_col_name = re.sub(r'[àâä]', 'a', new_col_name)
        new_col_name = re.sub(r'[ùûü]', 'u', new_col_name)
        new_col_name = re.sub(r'[îï]', 'i', new_col_name)
        new_col_name = re.sub(r'[ôö]', 'o', new_col_name)
        new_col_name = re.sub(r'[ç]', 'c', new_col_name)
        new_col_name = re.sub(r'[^a-z0-9_]', '_', new_col_name)
        new_col_name = re.sub(r'_+', '_', new_col_name)
        new_col_name = new_col_name.strip('_')
        df = df.withColumnRenamed(col_name, new_col_name)
    return df


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Supprime les lignes dupliquées
    """
    return df.dropDuplicates()


def handle_missing_values(df: DataFrame, strategy='drop', fill_value=None) -> DataFrame:
    """
    Gère les valeurs manquantes selon la stratégie choisie
    
    Args:
        df: DataFrame à traiter
        strategy: 'drop' pour supprimer, 'fill' pour remplir
        fill_value: valeur de remplacement si strategy='fill'
    """
    if strategy == 'drop':
        return df.dropna()
    elif strategy == 'fill' and fill_value is not None:
        return df.fillna(fill_value)
    return df


def normalize_commune_names(df: DataFrame, commune_col: str) -> DataFrame:
    """
    Normalise les noms de communes (majuscules, trim)
    """
    return df.withColumn(commune_col, upper(trim(col(commune_col))))


def cast_columns_types(df: DataFrame, type_mapping: dict) -> DataFrame:
    """
    Convertit les colonnes dans les types appropriés
    
    Args:
        df: DataFrame
        type_mapping: dict avec {nom_colonne: type_spark}
    """
    for col_name, col_type in type_mapping.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(col_type))
    return df


def filter_department(df: DataFrame, dept_col: str, dept_code: str) -> DataFrame:
    """
    Filtre les données pour un département spécifique
    
    Args:
        df: DataFrame
        dept_col: nom de la colonne département
        dept_code: code du département (ex: '34' pour Hérault)
    """
    return df.filter(col(dept_col) == dept_code)


def aggregate_by_commune(df: DataFrame, commune_col: str, agg_columns: dict) -> DataFrame:
    """
    Agrège les données par commune
    
    Args:
        df: DataFrame
        commune_col: colonne de regroupement
        agg_columns: dict avec {colonne: fonction d'agrégation} ex: {'population': 'sum'}
    """
    return df.groupBy(commune_col).agg(agg_columns)


def create_key_column(df: DataFrame, key_name: str, columns: list) -> DataFrame:
    """
    Crée une colonne clé en concaténant plusieurs colonnes
    
    Args:
        df: DataFrame
        key_name: nom de la nouvelle colonne
        columns: liste des colonnes à concaténer
    """
    from pyspark.sql.functions import concat_ws
    return df.withColumn(key_name, concat_ws('_', *[col(c) for c in columns]))


def standardize_values(df: DataFrame, column: str, mapping: dict) -> DataFrame:
    """
    Standardise les valeurs d'une colonne selon un mapping
    
    Args:
        df: DataFrame
        column: colonne à standardiser
        mapping: dict de correspondance ancienne_valeur -> nouvelle_valeur
    """
    for old_val, new_val in mapping.items():
        df = df.withColumn(column, when(col(column) == old_val, new_val).otherwise(col(column)))
    return df


def remove_outliers(df: DataFrame, column: str, lower_percentile=0.01, upper_percentile=0.99) -> DataFrame:
    """
    Supprime les valeurs aberrantes selon les percentiles
    
    Args:
        df: DataFrame
        column: colonne à nettoyer
        lower_percentile: percentile inférieur
        upper_percentile: percentile supérieur
    """
    bounds = df.approxQuantile(column, [lower_percentile, upper_percentile], 0.01)
    return df.filter((col(column) >= bounds[0]) & (col(column) <= bounds[1]))


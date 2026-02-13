"""
Module d'extraction des données
Charge les données depuis diverses sources (CSV, JSON, API, bases de données)
"""

from pyspark.sql import SparkSession
import os


def create_spark_session(app_name="ETL_Pipeline"):
    """
    Crée et retourne une session Spark
    
    Args:
        app_name: Nom de l'application Spark
    
    Returns:
        SparkSession: Session Spark configurée
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    return spark


def extract_csv(spark, file_path, **options):
    """
    Extrait les données d'un fichier CSV
    
    Args:
        spark: Session Spark
        file_path: Chemin du fichier CSV
        **options: Options de lecture (header, inferSchema, sep, etc.)
    
    Returns:
        DataFrame: Données extraites
    """
    default_options = {
        'header': True,
        'inferSchema': True,
        'sep': ','
    }
    default_options.update(options)
    
    df = spark.read.options(**default_options).csv(file_path)
    print(f"✓ Extraction réussie: {file_path}")
    print(f"  → {df.count()} lignes, {len(df.columns)} colonnes")
    
    return df


def extract_json(spark, file_path):
    """
    Extrait les données d'un fichier JSON
    
    Args:
        spark: Session Spark
        file_path: Chemin du fichier JSON
    
    Returns:
        DataFrame: Données extraites
    """
    df = spark.read.json(file_path)
    print(f"✓ Extraction JSON réussie: {file_path}")
    
    return df


def extract_parquet(spark, file_path):
    """
    Extrait les données d'un fichier Parquet
    
    Args:
        spark: Session Spark
        file_path: Chemin du fichier Parquet
    
    Returns:
        DataFrame: Données extraites
    """
    df = spark.read.parquet(file_path)
    print(f"✓ Extraction Parquet réussie: {file_path}")
    
    return df

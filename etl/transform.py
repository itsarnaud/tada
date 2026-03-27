"""
Module de transformation des données
Orchestre les différentes couches de traitement (Bronze, Silver, Gold)
"""

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.bronze.bronze import ingest_raw_data as process_bronze_layer
from etl.silver.silver import process_all_datasets as process_silver_layer
from etl.gold.gold import process_gold_layer


def run_bronze_layer():
    print("Lancement de la couche BRONZE...")
    result = process_bronze_layer()
    return result


def run_silver_layer():
    print("Lancement de la couche SILVER...")
    datasets = process_silver_layer()
    return datasets


def run_gold_layer():
    print("Lancement de la couche GOLD...")
    df = process_gold_layer()
    return df


def run_all_transformations():
    print("=" * 70)
    print("PIPELINE DE TRANSFORMATION - Architecture Medallion")
    print("=" * 70)

    run_bronze_layer()
    run_silver_layer()
    run_gold_layer()

    print("=" * 70)
    print("Pipeline de transformation termine")
    print("=" * 70)


if __name__ == "__main__":
    run_all_transformations()

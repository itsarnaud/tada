"""
Module de transformation des données
Orchestre les différentes couches de traitement (Bronze, Silver, Gold)
"""

import sys
import os

# Ajouter le dossier parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import des couches de traitement
from etl.bronze.bronze import process_all_datasets as process_bronze_layer


def run_bronze_layer():
    """
    Exécute la couche Bronze (ingestion des données brutes)
    """
    print("\n🥉 Lancement de la couche BRONZE...")
    datasets = process_bronze_layer()
    return datasets


def run_silver_layer():
    """
    Exécute la couche Silver (nettoyage et transformation)
    TODO: À implémenter
    """
    print("\n🥈 Couche SILVER - À implémenter")
    pass


def run_gold_layer():
    """
    Exécute la couche Gold (agrégation et données métier)
    TODO: À implémenter
    """
    print("\n🥇 Couche GOLD - À implémenter")
    pass


def run_all_transformations():
    """
    Exécute toutes les couches de transformation
    """
    print("\n" + "=" * 70)
    print("🔄 PIPELINE DE TRANSFORMATION - Architecture Medallion")
    print("=" * 70)
    
    # Bronze: Ingestion brute
    bronze_datasets = run_bronze_layer()
    
    # Silver: Nettoyage et enrichissement
    # run_silver_layer()
    
    # Gold: Données métier finales
    # run_gold_layer()
    
    print("\n" + "=" * 70)
    print("✅ Pipeline de transformation terminé")
    print("=" * 70)


if __name__ == "__main__":
    run_all_transformations()

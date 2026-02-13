"""
Configuration du projet ETL
"""

import os

# Chemins de base
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Chemins des données
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
FINAL_DATA_DIR = os.path.join(DATA_DIR, 'final')

# Configuration Spark
SPARK_CONFIG = {
    'app_name': 'ETL_Pipeline',
    'master': 'local[*]',
    'memory': '4g',
    'executor_cores': 2
}

# Options de lecture CSV par défaut
CSV_READ_OPTIONS = {
    'header': True,
    'inferSchema': True,
    'sep': ','
}

# Options d'écriture
WRITE_OPTIONS = {
    'mode': 'overwrite',
    'header': True
}

    ],
    "social": [
        "niveau_vie_median",
        "taux_pauvrete",
        "niveau_diplome",
        "aide_sociale"
    ],
    "logement": [
        "logements_vacants",
        "logements_sociaux",
        "proprietaires_vs_locataires",
        "evolution_construction"
    ],
    "education": [
        "taux_sans_diplome",
        "taux_diplome_superieur",
        "niveau_education_moyen"
    ]
}

## Justification des critères
"""
Ces indicateurs ont été choisis car ils présentent des corrélations fortes 
avec les comportements électoraux selon la littérature scientifique:

- PAUVRETÉ: Impact direct sur le vote protestataire et l'abstention
- EMPLOI: Corrélation avec la satisfaction vis-à-vis du gouvernement
- ÉDUCATION: Influence sur les choix politiques et la participation
- DÉMOGRAPHIE: Âge et composition sociale déterminent les orientations
- LOGEMENT: Reflet de la précarité et de la stabilité sociale
"""

## Configuration des chemins
PATHS = {
    "raw": "data/raw",
    "processed": "data/processed",
    "final": "data/final",
    "models": "models",
    "visualisations": "visualisations",
    "documentation": "documentation"
}

## Paramètres du modèle
MODEL_CONFIG = {
    "test_size": 0.2,
    "random_state": 42,
    "cv_folds": 5,
    "target_variable": "score_majorite",  # À définir selon les données
    "algorithms_to_test": [
        "RandomForest",
        "GradientBoosting",
        "LinearRegression",
        "XGBoost"
    ]
}

## Périodes de prédiction
PREDICTION_HORIZONS = [1, 2, 3]  # années

## Exports
EXPORT_FORMATS = ["CSV", "SQLite", "Parquet"]

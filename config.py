# Configuration du projet Electio-Analytics

## Périmètre géographique
DEPARTEMENT = "Hérault"
CODE_DEPARTEMENT = "34"
REGION = "Occitanie"

## Justification du choix
"""
Le département de l'Hérault (34) a été choisi pour ce POC pour les raisons suivantes:

1. DISPONIBILITÉ DES DONNÉES:
   - Données électorales complètes (municipales 2020)
   - Indicateurs socio-économiques détaillés (INSEE)
   - Données démographiques par commune
   - Statistiques sur l'emploi, le logement, l'éducation

2. REPRÉSENTATIVITÉ:
   - Population: ~1,2 million d'habitants
   - 342 communes de tailles variées
   - Mélange urbain/rural (Montpellier + zones rurales)
   - Diversité socio-économique importante

3. PERTINENCE ÉLECTORALE:
   - Département politiquement contrasté
   - Historique électoral riche
   - Variations importantes entre communes
   - Tendances électorales représentatives

4. VOLUMÉTRIE EXPLOITABLE:
   - Taille suffisante pour l'analyse statistique
   - Pas trop volumineuse pour le POC
   - Permet des modèles prédictifs robustes
"""

## Indicateurs retenus pour l'analyse

INDICATEURS = {
    "demographie": [
        "population_totale",
        "evolution_population",
        "densite_population",
        "repartition_par_age",
        "taux_vieillissement"
    ],
    "economie": [
        "taux_chomage",
        "population_active",
        "categories_socioprofessionnelles",
        "nombre_entreprises"
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

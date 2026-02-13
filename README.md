# � Projet ETL

Pipeline d'extraction, transformation et chargement de données avec PySpark.

## 📋 Structure du Projet

```
tada/
├── data/
│   ├── raw/              # Données sources brutes
│   ├── processed/        # Données intermédiaires
│   └── final/            # Données finales traitées
├── etl/
│   ├── extract.py        # Extraction des données
│   ├── transform.py      # Transformation et nettoyage
│   ├── load.py           # Chargement et sauvegarde
│   └── main.py           # Pipeline principal
├── models/               # Modèles (à venir)
├── config.py            # Configuration
└── requirements.txt     # Dépendances Python
```

## 🚀 Installation

1. Installer les dépendances:
```bash
pip install -r requirements.txt
```

2. Configurer Java (requis pour PySpark):
   - Installer JDK 8 ou 11
   - Définir `JAVA_HOME`

## 💻 Utilisation

### Lancer le pipeline ETL complet:

```bash
python etl/main.py
```

### Utiliser les modules individuellement:

```python
from etl.extract import create_spark_session, extract_csv
from etl.transform import clean_column_names, remove_duplicates
from etl.load import save_to_csv, save_to_parquet

# Initialiser Spark
spark = create_spark_session("MonApp")

# Extraire
df = extract_csv(spark, "data/raw/fichier.csv")

# Transformer
df = clean_column_names(df)
df = remove_duplicates(df)

# Charger
save_to_csv(df, "data/final/resultat.csv")
```

## 📦 Modules ETL

### `extract.py`
- `create_spark_session()` - Créer une session Spark
- `extract_csv()` - Charger des fichiers CSV
- `extract_json()` - Charger des fichiers JSON
- `extract_parquet()` - Charger des fichiers Parquet

### `transform.py`
- `clean_column_names()` - Nettoyer les noms de colonnes
- `remove_duplicates()` - Supprimer les doublons
- `handle_missing_values()` - Gérer les valeurs manquantes
- `cast_columns_types()` - Convertir les types de colonnes
- `filter_rows()` - Filtrer les lignes
- `aggregate_data()` - Agréger les données

### `load.py`
- `save_to_csv()` - Sauvegarder en CSV
- `save_to_parquet()` - Sauvegarder en Parquet
- `save_to_json()` - Sauvegarder en JSON
- `save_multiple_formats()` - Sauvegarder dans plusieurs formats

## 🔧 Configuration

Modifier les chemins dans [config.py](config.py) selon vos besoins.

## 📝 Notes

- Les données sources doivent être placées dans `data/raw/`
- Les résultats seront générés dans `data/final/`
- PySpark nécessite Java 8 ou 11

## 📄 Licence

MIT

│   └── final/                  # Base de données intégrée (SQLite)
│
├── etl/
│   ├── extract.py              # Fonctions d'extraction Spark
│   ├── transform.py            # Fonctions de transformation
│   ├── load.py                 # Fonctions de chargement
│   ├── main.py                 # Pipeline ETL principal
│   └── integrate.py            # Intégration et fusion des sources
│
├── notebooks/
│   ├── 01_analyse_exploratoire.ipynb   # EDA et corrélations
│   └── 02_modele_predictif.ipynb       # ML et prédictions
│
├── models/                     # Modèles ML sauvegardés
├── visualisations/             # Graphiques et cartes
├── sql/                        # Schémas de base de données
│
├── documentation/
│   └── 01_dossier_de_synthese.md   # Rapport complet du projet
│
├── config.py                   # Configuration du projet
└── README.md                   # Ce fichier

```

---

## 🚀 Installation

### Prérequis

- Python 3.11+
- Java 8+ (pour PySpark)
- Git

### Étapes d'installation

1. **Cloner le repository**
```bash
git clone <url-du-repo>
cd tada
```

2. **Créer un environnement virtuel**
```bash
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac
```

3. **Installer les dépendances**
```bash
pip install -r requirements.txt
```

### Dépendances principales

```txt
pandas>=2.0.0
numpy>=1.24.0
pyspark>=3.4.0
matplotlib>=3.7.0
seaborn>=0.12.0
scikit-learn>=1.3.0
jupyter>=1.0.0
sqlite3
openpyxl
```

---

## 💻 Utilisation

### 1. Exécuter le Pipeline ETL

```bash
# Nettoyer et transformer toutes les données
python etl/main.py

# Intégrer les données dans une base SQLite
python etl/integrate.py
```

**Résultat :** 
- Fichiers CSV nettoyés dans `data/processed/`
- Base SQLite dans `data/final/electio_analytics.db`

### 2. Lancer l'Analyse Exploratoire

```bash
jupyter notebook notebooks/01_analyse_exploratoire.ipynb
```

**Contenu :**
- Chargement et exploration des données
- Statistiques descriptives
- Analyse de corrélation
- Visualisations (histogrammes, heatmaps, cartes)

### 3. Entraîner le Modèle Prédictif

```bash
jupyter notebook notebooks/02_modele_predictif.ipynb
```

**Contenu :**
- Préparation des features
- Train/Test split
- Entraînement de modèles (Random Forest, Gradient Boosting, etc.)
- Évaluation des performances (R², RMSE, MAE)
- Prédictions à 1, 2 et 3 ans

---

## 📊 Structure des Données

### Sources de Données (Raw)

| Fichier | Description | Source |
|---------|-------------|--------|
| `DT PRINCIPAL elections-municipales-2020-resultats-du-2eme-tour.csv` | Résultats électoraux 2020 | Ministère Intérieur |
| `evolution-de-la-population-par-tranches-dage-dans-lherault.csv` | Démographie par âge | INSEE |
| `logements-par-categorie-herault.csv` | Statistiques logement | INSEE |
| `Niveau de vie median et taux de pauvrete par type de menage.csv` | Indicateurs de pauvreté | INSEE |
| `niveau-de-diplome-de-la-population-herault.csv` | Niveau d'éducation | INSEE |
| `population-active-herault.csv` | Emploi et chômage | INSEE |
| `population par categorie socioprofessionelle.csv` | CSP | INSEE |
| `communes-france-2025 (1).csv` | Référentiel communes | Data.gouv.fr |

### Base de Données Finale

**Format :** SQLite  
**Localisation :** `data/final/electio_analytics.db`

**Tables principales :**
- `communes` : Référentiel des communes de l'Hérault
- `elections_2020` : Résultats électoraux
- `demographie` : Données de population par âge
- `pauvrete` : Niveau de vie et taux de pauvreté
- `education` : Niveau de diplôme
- `emploi` : Population active et chômage
- `logement` : Statistiques de logement
- `sociopro` : Catégories socioprofessionnelles

---

## 🔄 Pipeline ETL

### Extraction (`extract.py`)

- Chargement des fichiers CSV via PySpark
- Détection automatique du schéma
- Gestion des erreurs de format

### Transformation (`transform.py`)

**Fonctions principales :**
- `clean_column_names()` : Normalisation des noms de colonnes
- `remove_duplicates()` : Suppression des doublons
- `handle_missing_values()` : Gestion des valeurs manquantes
- `normalize_commune_names()` : Standardisation des noms de communes
- `cast_columns_types()` : Conversion des types de données
- `filter_department()` : Filtrage pour l'Hérault (34)

### Chargement (`load.py`)

**Formats d'export :**
- CSV : Format universel
- SQLite : Base relationnelle pour analyses
- Parquet : Format optimisé pour big data

### Orchestration (`main.py`)

Pipeline complet automatisé :
1. Chargement de toutes les sources
2. Nettoyage et normalisation
3. Export des données traitées
4. Génération de rapports de synthèse

---

## 🤖 Modèle Prédictif

### Approche

- **Type :** Apprentissage supervisé (régression)
- **Variable cible (Y) :** Score de la liste majoritaire (%)
- **Features (X) :** Indicateurs socio-économiques

### Modèles Testés

1. **Linear Regression** : Baseline simple
2. **Random Forest** : Ensemble d'arbres de décision
3. **Gradient Boosting** : Boosting séquentiel
4. **XGBoost** : Optimisation avancée

### Métriques d'Évaluation

- **R² (R-squared)** : Proportion de variance expliquée
- **RMSE** : Erreur quadratique moyenne
- **MAE** : Erreur absolue moyenne
- **Cross-validation** : Validation croisée 5-folds

### Résultats

*À compléter après exécution du notebook de modélisation*

---

## 📦 Livrables

### 1. Dossier de Synthèse

**Localisation :** `documentation/01_dossier_de_synthese.md`

**Contenu :**
- Justification du périmètre géographique
- Choix et justification des critères
- Démarche méthodologique
- Modèle Conceptuel de Données (MCD)
- Modèles testés et résultats
- Visualisations
- Accuracy et performance
- Réponses aux questions d'analyse

### 2. Jeu de Données Nettoyé

**Localisation :** `data/final/electio_analytics.db`

**Format :** SQLite (compatible SQL)

**Schéma :** `sql/schema.sql`

### 3. Code Source Commenté

- **ETL :** `etl/`
- **Notebooks :** `notebooks/`
- **Configuration :** `config.py`

Tous les scripts sont documentés avec docstrings et commentaires.

### 4. Support de Présentation

*À créer pour la soutenance (PowerPoint ou équivalent)*

**Durée :** 20 minutes de présentation + 30 minutes d'échanges

---

## 🔍 Questions d'Analyse

### Q1 : Quelle donnée est la plus corrélée aux résultats électoraux ?

**Réponse :** Voir le notebook `01_analyse_exploratoire.ipynb` et le dossier de synthèse.

La matrice de corrélation révèle que **[INDICATEUR]** présente la corrélation la plus forte (r = [XX]).

### Q2 : Définissez le principe d'un apprentissage supervisé

**Réponse :** L'apprentissage supervisé est une méthode de machine learning où l'algorithme apprend à partir de données étiquetées (avec résultats connus) pour prédire des résultats futurs.

**Dans notre projet :**
- **Données d'entraînement :** Résultats électoraux 2020 + indicateurs socio-économiques
- **Modèle :** Apprend les relations entre indicateurs et résultats
- **Prédiction :** Résultats électoraux futurs (2026-2028)

Voir détails dans `documentation/01_dossier_de_synthese.md`.

### Q3 : Comment définissez-vous la précision (accuracy) du modèle ?

**Réponse :** Pour un modèle de régression, nous utilisons :

- **R²** : Proportion de variance expliquée (0 à 1)
- **RMSE** : Erreur quadratique moyenne (en points de %)
- **MAE** : Erreur absolue moyenne (en points de %)

**Exemple d'interprétation :**
- R² = 0.85 → Le modèle explique 85% de la variabilité
- MAE = 3.2 → Erreur moyenne de ±3.2 points de %

Voir résultats détaillés dans `notebooks/02_modele_predictif.ipynb`.

---

## 👥 Équipe

**Membres du projet :**
- [Nom 1] - Chef de projet / Data Scientist
- [Nom 2] - Data Engineer
- [Nom 3] - Data Analyst
- [Nom 4] - Visualisation / Reporting

**Client :** Electio-Analytics  
**Période :** Janvier 2026  
**Durée :** 25 heures de préparation

---

## 📚 Ressources Complémentaires

### Documentation

- [Dossier de synthèse complet](documentation/01_dossier_de_synthese.md)
- [Schéma de base de données](sql/schema.sql)
- [Configuration du projet](config.py)

### Liens Externes

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Scikit-learn User Guide](https://scikit-learn.org/stable/user_guide.html)
- [INSEE - Données communales](https://www.insee.fr)
- [Data.gouv.fr - Open Data](https://www.data.gouv.fr)

### Gestion de Projet

Lien du Trello : <https://trello.com/invite/b/6964c47ad0851811af02e0f1/ATTI4561ee92c155d0309722f321b591ce4757F69540/mspr-big-data-analyse-de-donnees>

---

## 📄 Licence

Ce projet est développé dans le cadre d'une MSPR académique.

---

## 📞 Contact

Pour toute question sur le projet :
- **Email :** [email@example.com]
- **GitHub :** [lien-github]

---

**Dernière mise à jour :** Janvier 2026

"""
Pipeline ETL principal pour le projet Electio-Analytics - Hérault
Extraction, Transformation et Chargement des données électorales et socio-économiques
"""

import pandas as pd
from extract import create_spark_session, read_csv
from transform import (
    clean_column_names, 
    remove_duplicates, 
    handle_missing_values,
    normalize_commune_names,
    cast_columns_types,
    filter_department
)
from load import save_to_csv, save_to_parquet, create_summary_report
from pyspark.sql.types import IntegerType, DoubleType, StringType
import os

# Configuration
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_PATH, 'data', 'raw')
PROCESSED_DATA_PATH = r'C:\Users\theop\Desktop\data\clean'
FINAL_DATA_PATH = os.path.join(BASE_PATH, 'data', 'final')

# Créer les dossiers si nécessaire
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(FINAL_DATA_PATH, exist_ok=True)

# Initialiser Spark
print("🚀 Démarrage du pipeline ETL Electio-Analytics")
print("=" * 60)
spark = create_spark_session('TADA_ElectioAnalytics')


def process_elections_data():
    """
    Traite les données des élections municipales 2020
    """
    print("\n📊 Traitement des données électorales...")
    
    elections_file = os.path.join(RAW_DATA_PATH, 'DT PRINCIPAL elections-municipales-2020-resultats-du-2eme-tour.csv')
    
    # Extraction
    df_elections = read_csv(spark, elections_file, sep=';')
    print(f"  → {df_elections.count()} lignes chargées (bureaux de vote)")
    
    # Transformation
    df_elections = clean_column_names(df_elections)
    df_elections = remove_duplicates(df_elections)
    
    print(f"  → Aperçu des colonnes: {', '.join(df_elections.columns[:15])}")
    
    # Identifier les colonnes importantes
    code_commune_col = None
    nom_commune_col = None
    nom_candidat_col = None
    prenom_candidat_col = None
    code_nuance_col = None
    
    # Chercher colonne code commune
    for col in df_elections.columns:
        col_lower = col.lower()
        if 'code' in col_lower and 'commune' in col_lower and 'officiel' in col_lower:
            code_commune_col = col
            break
    
    # Chercher colonne nom commune
    for col in df_elections.columns:
        col_lower = col.lower()
        if 'nom' in col_lower and 'commune' in col_lower and 'officiel' in col_lower:
            nom_commune_col = col
            break
    
    # Chercher colonne nom candidat
    for col in df_elections.columns:
        col_lower = col.lower()
        if col_lower == 'nom':
            nom_candidat_col = col
            break
    
    # Chercher colonne prénom candidat
    for col in df_elections.columns:
        col_lower = col.lower()
        if 'prenom' in col_lower or 'prénom' in col_lower:
            prenom_candidat_col = col
            break
    
    # Chercher colonne code nuance
    for col in df_elections.columns:
        col_lower = col.lower()
        if 'code' in col_lower and 'nuance' in col_lower:
            code_nuance_col = col
            break
    
    print(f"  → Colonnes identifiées:")
    print(f"     - Code commune: {code_commune_col}")
    print(f"     - Nom commune: {nom_commune_col}")
    print(f"     - Nom candidat: {nom_candidat_col}")
    print(f"     - Prénom candidat: {prenom_candidat_col}")
    print(f"     - Code nuance: {code_nuance_col}")
    
    if code_commune_col and nom_commune_col and nom_candidat_col and prenom_candidat_col:
        print(f"  → Regroupement par commune + candidat")
        
        # Convertir en Pandas pour l'agrégation
        df_pandas = df_elections.toPandas()
        
        # Identifier la colonne des voix
        voix_col = None
        for col in df_pandas.columns:
            col_lower = col.lower()
            if col_lower == 'voix':  # Exactement "voix"
                voix_col = col
                break
        
        if not voix_col:
            # Chercher colonne contenant "voix" mais pas "exp" ou "%"
            for col in df_pandas.columns:
                col_lower = col.lower()
                if 'voix' in col_lower and 'exp' not in col_lower and '%' not in col_lower:
                    voix_col = col
                    break
        
        print(f"  → Colonne des voix: {voix_col}")
        
        # Vérifier que les colonnes existent
        if voix_col not in df_pandas.columns:
            print(f"  ⚠️ Erreur: colonne {voix_col} introuvable")
            return df_elections
        
        # Agrégation: code + nom commune + nom + prénom candidat + code nuance → somme des voix
        group_cols = [code_commune_col, nom_commune_col, nom_candidat_col, prenom_candidat_col]
        if code_nuance_col:
            group_cols.append(code_nuance_col)
        
        print(f"  → Groupement par: {group_cols}")
        df_aggregated = df_pandas.groupby(group_cols, as_index=False)[voix_col].sum()
        
        # Créer la colonne orientation politique
        if code_nuance_col and code_nuance_col in df_aggregated.columns:
            print(f"  → Ajout de la colonne 'orientation_politique'")
            
            def categoriser_orientation(code):
                if pd.isna(code):
                    return 'INCONNU'
                code = str(code).strip().upper()
                
                # GAUCHE
                if code in ['LEXG', 'LCOP', 'LCOM', 'LSOC', 'LVEC', 'LDVG', 'LUG']:
                    return 'GAUCHE'
                # DROITE
                elif code in ['LCMD', 'LMMD', 'LMAJ', 'LDVD', 'LFN', 'LEXD']:
                    return 'DROITE'
                # CENTRE
                elif code in ['DIV', 'REG']:
                    return 'CENTRE'
                else:
                    return 'AUTRE'
            
            df_aggregated['orientation_politique'] = df_aggregated[code_nuance_col].apply(categoriser_orientation)
            print(f"  ✓ Orientations ajoutées:")
            print(df_aggregated['orientation_politique'].value_counts())
        
        print(f"  ✓ Bureaux regroupés: {len(df_pandas)} lignes (bureaux × candidats) → {len(df_aggregated)} lignes (communes × candidats)")
        
        # Sauvegarder
        output_path = os.path.join(PROCESSED_DATA_PATH, 'elections_2020_clean.csv')
        df_aggregated.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"  ✓ Données électorales traitées et sauvegardées")
        print(f"  → Format: Code Commune | Nom Commune | Nom | Prénom | Voix\n")
        print(df_aggregated.head(10))
        
        # Convertir en Spark DataFrame pour retourner
        df_elections = spark.createDataFrame(df_aggregated)
    else:
        print(f"  ⚠️ Colonne commune non trouvée, sauvegarde sans agrégation")
        output_path = os.path.join(PROCESSED_DATA_PATH, 'elections_2020_clean.csv')
        df_elections.toPandas().to_csv(output_path, index=False, encoding='utf-8')
        print(f"  ✓ Données électorales traitées et sauvegardées")
    
    return df_elections


def process_population_data():
    """
    Traite les données de population par tranche d'âge
    Garde uniquement 2016 et 2022, puis interpole pour 2020
    """
    print("\n👥 Traitement des données de population...")
    
    pop_file = os.path.join(RAW_DATA_PATH, 'evolution-de-la-population-par-tranches-dage-dans-lherault.csv')
    
    df_pop = read_csv(spark, pop_file, sep=';')
    print(f"  → {df_pop.count()} lignes chargées")
    
    df_pop = clean_column_names(df_pop)
    df_pop = remove_duplicates(df_pop)
    
    # Convertir en Pandas pour faciliter le traitement
    df_pandas = df_pop.toPandas()
    
    # Identifier la colonne année
    annee_col = None
    for col in df_pandas.columns:
        if 'annee' in col.lower() or 'année' in col.lower():
            annee_col = col
            break
    
    if annee_col:
        print(f"  → Colonne année: {annee_col}")
        
        # Filtrer uniquement 2016 et 2022
        df_pandas[annee_col] = pd.to_numeric(df_pandas[annee_col], errors='coerce')
        df_filtered = df_pandas[df_pandas[annee_col].isin([2016, 2022])].copy()
        
        print(f"  → {len(df_filtered)} lignes conservées (2016 et 2022)")
        
        # Identifier colonnes de regroupement (commune, tranche d'âge, etc.)
        code_commune_col = None
        nom_commune_col = None
        tranche_age_col = None
        population_col = None
        
        for col in df_filtered.columns:
            col_lower = col.lower()
            if 'code' in col_lower and 'geo' in col_lower:
                code_commune_col = col
            elif 'nom' in col_lower and 'commune' in col_lower:
                nom_commune_col = col
            elif 'tranche' in col_lower or 'age' in col_lower:
                tranche_age_col = col
            elif 'population' in col_lower:
                population_col = col
        
        print(f"  → Code commune: {code_commune_col}, Nom commune: {nom_commune_col}, Tranche: {tranche_age_col}, Population: {population_col}")
        
        if code_commune_col and nom_commune_col and tranche_age_col and population_col and annee_col:
            # Convertir population en numérique
            df_filtered[population_col] = pd.to_numeric(df_filtered[population_col], errors='coerce')
            
            # Pivoter pour avoir 2016 et 2022 en colonnes
            df_pivot = df_filtered.pivot_table(
                index=[code_commune_col, nom_commune_col, tranche_age_col],
                columns=annee_col,
                values=population_col,
                aggfunc='sum'
            ).reset_index()
            
            # Renommer les colonnes d'année
            df_pivot.columns = [str(col) if col not in [code_commune_col, nom_commune_col, tranche_age_col] else col for col in df_pivot.columns]
            
            # Interpolation linéaire pour 2020
            # 2020 = 2016 + (2022 - 2016) * (2020-2016)/(2022-2016)
            # 2020 = 2016 + (2022 - 2016) * 4/6
            if '2016' in df_pivot.columns and '2022' in df_pivot.columns:
                df_pivot['2020_estimation'] = df_pivot['2016'] + (df_pivot['2022'] - df_pivot['2016']) * (4/6)
                
                print(f"  ✓ Estimation 2020 calculée par interpolation linéaire")
                print(f"  → Formule: 2020_estimation = 2016 + (2022 - 2016) × 0.667")
                
                # Garder uniquement les colonnes nécessaires (sans 2016 et 2022)
                cols = [code_commune_col, nom_commune_col, tranche_age_col, '2020_estimation']
                df_result = df_pivot[cols]
                
                # Regrouper certaines tranches d'âges
                print(f"  → Regroupement des tranches d'âges...")
                
                # Créer un mapping pour regrouper les tranches
                def regrouper_tranche(tranche):
                    tranche_str = str(tranche).strip()
                    if '30' in tranche_str and '44' in tranche_str:
                        return '30-59'
                    elif '45' in tranche_str and '59' in tranche_str:
                        return '30-59'
                    elif '60' in tranche_str and '74' in tranche_str:
                        return '60+'
                    elif '75' in tranche_str and '89' in tranche_str:
                        return '60+'
                    elif '90' in tranche_str:
                        return '60+'
                    else:
                        return tranche_str
                
                # Appliquer le regroupement
                df_result['tranche_regroupee'] = df_result[tranche_age_col].apply(regrouper_tranche)
                
                # Agréger par commune et nouvelle tranche
                df_result = df_result.groupby([code_commune_col, nom_commune_col, 'tranche_regroupee'], as_index=False)['2020_estimation'].sum()
                
                # Convertir en entier (arrondir)
                df_result['2020_estimation'] = df_result['2020_estimation'].round().astype(int)
                
                # Renommer la colonne
                df_result.rename(columns={'tranche_regroupee': tranche_age_col}, inplace=True)
                
                print(f"  ✓ Tranches d'âges regroupées:")
                print(f"     - 30-44 + 45-59 → 30-59")
                print(f"     - 60-74 + 75-89 + 90+ → 60+")
                print(f"  ✓ Populations arrondies en entiers")
                
                # Sauvegarder
                output_path = os.path.join(PROCESSED_DATA_PATH, 'population_age_clean.csv')
                df_result.to_csv(output_path, index=False, encoding='utf-8')
                
                print(f"  ✓ Données de population traitées et sauvegardées")
                print(f"  → Aperçu:\n")
                print(df_result.head(10))
                
                # Convertir en Spark DataFrame
                df_pop = spark.createDataFrame(df_result)
            else:
                print("  ⚠️ Colonnes 2016 ou 2022 manquantes")
        else:
            print("  ⚠️ Colonnes essentielles non trouvées")
    else:
        print("  ⚠️ Colonne année non trouvée")
    
    return df_pop


def process_education_data():
    """
    Traite les données de niveau de diplôme
    Regroupe hommes et femmes ensemble par commune et diplôme
    """
    print("\n🎓 Traitement des données d'éducation...")
    
    edu_file = os.path.join(RAW_DATA_PATH, 'niveau-de-diplome-de-la-population-herault.csv')
    
    df_edu = read_csv(spark, edu_file, sep=';')
    print(f"  → {df_edu.count()} lignes chargées")
    
    df_edu = clean_column_names(df_edu)
    df_edu = remove_duplicates(df_edu)
    
    # Convertir en Pandas pour faciliter le traitement
    df_pandas = df_edu.toPandas()
    
    # Identifier les colonnes importantes
    code_commune_col = None
    nom_commune_col = None
    diplome_col = None
    population_col = None
    
    for col in df_pandas.columns:
        col_lower = col.lower()
        if 'codgeo' in col_lower or ('code' in col_lower and 'geo' in col_lower):
            code_commune_col = col
        elif 'nom' in col_lower and 'commune' in col_lower:
            nom_commune_col = col
        elif 'diplome' in col_lower or 'diplôme' in col_lower:
            diplome_col = col
        elif 'population' in col_lower:
            population_col = col
    
    print(f"  → Code commune: {code_commune_col}")
    print(f"  → Nom commune: {nom_commune_col}")
    print(f"  → Diplôme: {diplome_col}")
    print(f"  → Population: {population_col}")
    
    if code_commune_col and nom_commune_col and diplome_col and population_col:
        # Convertir population en numérique
        df_pandas[population_col] = pd.to_numeric(df_pandas[population_col], errors='coerce')
        
        # Regrouper par commune et diplôme (sans distinction de sexe)
        print(f"  → Regroupement hommes + femmes par commune et diplôme...")
        df_aggregated = df_pandas.groupby([code_commune_col, nom_commune_col, diplome_col], as_index=False)[population_col].sum()
        
        # Regrouper les niveaux de diplôme en catégories
        print(f"  → Regroupement des niveaux de diplôme en catégories...")
        
        def regrouper_diplome(diplome):
            diplome_str = str(diplome).lower().strip()
            
            # Brevet ou sans diplome
            if any(x in diplome_str for x in ['bepc', 'brevet', 'dnb', 'sans diplome', 'cep']):
                return 'Brevet ou sans diplome'
            # Bac ou CAP/BEP
            elif any(x in diplome_str for x in ['bac', 'cap', 'bep', 'brevet pro']):
                if 'bac + 2' not in diplome_str and 'bac + 3' not in diplome_str and 'bac + 4' not in diplome_str and 'bac + 5' not in diplome_str:
                    return 'Bac ou CAP/BEP'
            # Enseignement sup bac +2 à 4
            if 'bac + 2' in diplome_str or 'bac + 3' in diplome_str or 'bac + 4' in diplome_str:
                return 'Enseignement sup de niveau bac +2 à 4'
            # Enseignement sup bac +5 ou plus
            elif 'bac + 5' in diplome_str or 'bac +5' in diplome_str:
                return 'Enseignement sup de niveau bac +5 ou plus'
            
            return diplome  # Garder tel quel si pas de correspondance
        
        # Appliquer le regroupement
        df_aggregated['diplome_regroupe'] = df_aggregated[diplome_col].apply(regrouper_diplome)
        
        # Agréger à nouveau par commune et diplôme regroupé
        df_aggregated = df_aggregated.groupby([code_commune_col, nom_commune_col, 'diplome_regroupe'], as_index=False)[population_col].sum()
        
        print(f"  ✓ Diplômes regroupés en 4 catégories:")
        print(f"     - Brevet ou sans diplome")
        print(f"     - Bac ou CAP/BEP")
        print(f"     - Enseignement sup de niveau bac +2 à 4")
        print(f"     - Enseignement sup de niveau bac +5 ou plus")
        
        # Garder uniquement les 4 colonnes nécessaires et renommer
        df_result = df_aggregated[[code_commune_col, nom_commune_col, 'diplome_regroupe', population_col]].copy()
        df_result.columns = ['code_commune', 'nom_commune', 'niveau_diplome', 'population_totale']
        
        print(f"  ✓ {len(df_pandas)} lignes → {len(df_result)} lignes (hommes + femmes agrégés)")
        print(f"  ✓ 4 colonnes conservées: code_commune, nom_commune, niveau_diplome, population_totale")
        
        # Sauvegarder
        output_path = os.path.join(PROCESSED_DATA_PATH, 'education_clean.csv')
        df_result.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"  ✓ Données d'éducation traitées et sauvegardées")
        print(f"  → Aperçu:\n")
        print(df_result.head(10))
        
        # Convertir en Spark DataFrame
        df_edu = spark.createDataFrame(df_result)
    else:
        print("  ⚠️ Colonnes essentielles non trouvées, sauvegarde sans traitement")
        output_path = os.path.join(PROCESSED_DATA_PATH, 'education_clean.csv')
        df_pandas.to_csv(output_path, index=False, encoding='utf-8')
    
    return df_edu


def process_employment_data():
    """
    Traite les données de population active
    Calcule le taux d'emploi et le taux de chômage par commune
    """
    print("\n💼 Traitement des données d'emploi...")
    
    emp_file = os.path.join(RAW_DATA_PATH, 'population-active-herault.csv')
    
    df_emp = read_csv(spark, emp_file, sep=';')
    print(f"  → {df_emp.count()} lignes chargées")
    
    df_emp = clean_column_names(df_emp)
    df_emp = remove_duplicates(df_emp)
    
    # Convertir en Pandas pour faciliter le traitement
    df_pandas = df_emp.toPandas()
    
    # Identifier les colonnes importantes
    code_commune_col = None
    nom_commune_col = None
    pop_active_col = None
    pop_active_occupee_col = None
    
    for col in df_pandas.columns:
        col_lower = col.lower()
        if 'codgeo' in col_lower or ('code' in col_lower and 'geo' in col_lower):
            code_commune_col = col
        elif 'nom' in col_lower and 'commune' in col_lower:
            nom_commune_col = col
        elif 'actives_occupees' in col_lower or ('actives' in col_lower and 'occupees' in col_lower):
            pop_active_occupee_col = col
        elif 'actives' in col_lower and 'occupees' not in col_lower:
            pop_active_col = col
    
    print(f"  → Code commune: {code_commune_col}")
    print(f"  → Nom commune: {nom_commune_col}")
    print(f"  → Population active: {pop_active_col}")
    print(f"  → Population active occupée: {pop_active_occupee_col}")
    
    if code_commune_col and nom_commune_col and pop_active_col and pop_active_occupee_col:
        # Convertir en numérique
        df_pandas[pop_active_col] = pd.to_numeric(df_pandas[pop_active_col], errors='coerce')
        df_pandas[pop_active_occupee_col] = pd.to_numeric(df_pandas[pop_active_occupee_col], errors='coerce')
        
        # Créer un nouveau dataframe avec les colonnes nécessaires
        df_result = df_pandas[[code_commune_col, nom_commune_col, pop_active_col, pop_active_occupee_col]].copy()
        
        # Calculer le taux de chômage: (population active - population active occupée) / population active
        df_result['taux_chomage'] = ((df_result[pop_active_col] - df_result[pop_active_occupee_col]) / df_result[pop_active_col] * 100).round(2)
        
        # Garder uniquement les colonnes nécessaires
        df_result = df_result[[code_commune_col, nom_commune_col, 'taux_chomage']].copy()
        df_result.columns = ['code_commune', 'nom_commune', 'taux_chomage']
        
        print(f"  ✓ Calcul du taux de chômage effectué:")
        print(f"     - Formule: ((Population active - Population active occupée) / Population active) × 100")
        print(f"  → Aperçu des statistiques:")
        print(f"     - Taux de chômage moyen: {df_result['taux_chomage'].mean():.2f}%")
        print(f"  ✓ 3 colonnes conservées: code_commune, nom_commune, taux_chomage")
        
        # Sauvegarder
        output_path = os.path.join(PROCESSED_DATA_PATH, 'emploi_clean.csv')
        df_result.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"  ✓ Données d'emploi traitées et sauvegardées")
        print(f"  → Aperçu:\n")
        print(df_result.head(10))
        
        # Convertir en Spark DataFrame
        df_emp = spark.createDataFrame(df_result)
    else:
        print("  ⚠️ Colonnes essentielles non trouvées, sauvegarde sans traitement")
        output_path = os.path.join(PROCESSED_DATA_PATH, 'emploi_clean.csv')
        df_pandas.to_csv(output_path, index=False, encoding='utf-8')
    
    return df_emp


def process_sociopro_data():
    """
    Traite les données de catégories socioprofessionnelles
    Regroupe les CSP en 4 grandes catégories
    """
    print("\n👔 Traitement des données socioprofessionnelles...")
    
    socio_file = os.path.join(RAW_DATA_PATH, 'population par categorie socioprofessionelle.csv')
    
    df_socio = read_csv(spark, socio_file, sep=';')
    print(f"  → {df_socio.count()} lignes chargées")
    
    df_socio = clean_column_names(df_socio)
    df_socio = remove_duplicates(df_socio)
    
    # Convertir en Pandas pour faciliter le traitement
    df_pandas = df_socio.toPandas()
    
    # Identifier les colonnes importantes
    code_commune_col = None
    nom_commune_col = None
    csp_col = None
    population_col = None
    
    for col in df_pandas.columns:
        col_lower = col.lower()
        if 'codgeo' in col_lower or ('code' in col_lower and 'geo' in col_lower):
            code_commune_col = col
        elif 'nom' in col_lower and 'commune' in col_lower:
            nom_commune_col = col
        elif 'csp' in col_lower or 'socioprofession' in col_lower or 'categorie' in col_lower:
            csp_col = col
        elif 'population' in col_lower:
            population_col = col
    
    print(f"  → Code commune: {code_commune_col}")
    print(f"  → Nom commune: {nom_commune_col}")
    print(f"  → CSP: {csp_col}")
    print(f"  → Population: {population_col}")
    
    if code_commune_col and nom_commune_col and csp_col and population_col:
        # Convertir population en numérique
        df_pandas[population_col] = pd.to_numeric(df_pandas[population_col], errors='coerce')
        
        # Regrouper les CSP en 4 grandes catégories
        print(f"  → Regroupement des CSP en 4 grandes catégories...")
        
        def regrouper_csp(csp):
            csp_str = str(csp).lower().strip()
            
            # Indépendants = Artisans, Comm., Chefs entr. + Agriculteurs exploitants
            if 'artisan' in csp_str or 'comm.' in csp_str or 'chef' in csp_str or 'agriculteur' in csp_str or 'exploitant' in csp_str:
                return 'Indépendants'
            # Actifs qualifiés = Prof. intermédiaires + Cadres, Prof. intel. sup.
            elif 'prof' in csp_str and 'intermédiaire' in csp_str:
                return 'Actifs qualifiés'
            elif 'cadre' in csp_str or 'intel' in csp_str:
                return 'Actifs qualifiés'
            # Inactifs = Retraités + Autres
            elif 'retraité' in csp_str or 'retraite' in csp_str or 'autre' in csp_str:
                return 'Inactifs'
            # Actifs populaires = Ouvriers + Employés
            elif 'ouvrier' in csp_str or 'employé' in csp_str or 'employe' in csp_str:
                return 'Actifs populaires'
            else:
                return 'Autres'  # Pour les CSP non identifiées
        
        # Appliquer le regroupement
        df_pandas['csp_regroupee'] = df_pandas[csp_col].apply(regrouper_csp)
        
        # Agréger par commune et CSP regroupée
        df_aggregated = df_pandas.groupby([code_commune_col, nom_commune_col, 'csp_regroupee'], as_index=False)[population_col].sum()
        
        # Arrondir les populations en entiers
        df_aggregated[population_col] = df_aggregated[population_col].round().astype(int)
        
        print(f"  ✓ CSP regroupées en 4 catégories:")
        print(f"     - Indépendants = Artisans, Comm., Chefs entr. + Agriculteurs exploitants")
        print(f"     - Actifs qualifiés = Prof. intermédiaires + Cadres, Prof. intel. sup.")
        print(f"     - Inactifs = Retraités + Autres")
        print(f"     - Actifs populaires = Ouvriers + Employés")
        print(f"  ✓ Populations arrondies en entiers")
        
        # Garder uniquement les colonnes nécessaires et renommer
        df_result = df_aggregated[[code_commune_col, nom_commune_col, 'csp_regroupee', population_col]].copy()
        df_result.columns = ['code_commune', 'nom_commune', 'categorie_sociopro', 'population']
        
        print(f"  ✓ {len(df_pandas)} lignes → {len(df_result)} lignes (CSP regroupées)")
        print(f"  ✓ 4 colonnes conservées: code_commune, nom_commune, categorie_sociopro, population")
        
        # Sauvegarder
        output_path = os.path.join(PROCESSED_DATA_PATH, 'sociopro_clean.csv')
        df_result.to_csv(output_path, index=False, encoding='utf-8')
        
        print(f"  ✓ Données socioprofessionnelles traitées et sauvegardées")
        print(f"  → Aperçu:\n")
        print(df_result.head(10))
        
        # Convertir en Spark DataFrame
        df_socio = spark.createDataFrame(df_result)
    else:
        print("  ⚠️ Colonnes essentielles non trouvées, sauvegarde sans traitement")
        output_path = os.path.join(PROCESSED_DATA_PATH, 'sociopro_clean.csv')
        df_pandas.to_csv(output_path, index=False, encoding='utf-8')
    
    return df_socio


def process_communes_data():
    """
    Traite les données de référence des communes
    """
    print("\n🗺️  Traitement des données des communes...")
    
    communes_file = os.path.join(RAW_DATA_PATH, 'communes-france-2025 (1).csv')
    
    df_communes = read_csv(spark, communes_file, sep=',')
    print(f"  → {df_communes.count()} lignes chargées")
    
    df_communes = clean_column_names(df_communes)
    df_communes = remove_duplicates(df_communes)
    
    # Filtrer pour l'Hérault (département 34)
    if 'code_departement' in df_communes.columns:
        df_communes = df_communes.filter(df_communes.code_departement == '34')
    elif 'dep' in df_communes.columns:
        df_communes = df_communes.filter(df_communes.dep == '34')
    
    print(f"  → {df_communes.count()} communes dans l'Hérault")
    
    output_path = os.path.join(PROCESSED_DATA_PATH, 'communes_herault_clean.csv')
    df_communes.toPandas().to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"  ✓ Données des communes traitées et sauvegardées")
    return df_communes


if __name__ == "__main__":
    try:
        # Exécuter tous les traitements
        df_elections = process_elections_data()
        df_population = process_population_data()
        df_education = process_education_data()
        df_employment = process_employment_data()
        df_sociopro = process_sociopro_data()
        #df_communes = process_communes_data()
        
        print("\n" + "=" * 60)
        print("✅ Pipeline ETL terminé avec succès!")
        print(f"📁 Données traitées disponibles dans: {PROCESSED_DATA_PATH}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution du pipeline: {str(e)}")
        raise
    finally:
        spark.stop()
        print("\n🛑 Session Spark fermée")


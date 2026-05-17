"""
ENTRAÎNEMENT DES MODÈLES DE PRÉDICTION ÉLECTORALE

Approche chronologique correcte :
- Train : 2014 + 2019 (passé)
- Test : 2024 (futur)

Stratégie anti-overfitting :
- Validation croisée 5-fold
- Régularisation des hyperparamètres
- Split train/validation
- Normalisation des features
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score, KFold
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import joblib
import os

print("=" * 80)
print("ENTRAÎNEMENT DES MODÈLES - APPROCHE CHRONOLOGIQUE")
print("Train : 2014 + 2019  →  Test : 2024")
print("=" * 80)

# ============================================================================
# CHARGEMENT DES DONNÉES
# ============================================================================
print("\n[1] CHARGEMENT DES DONNÉES...")

df_2014 = pd.read_csv('./data/gold/departements_2014.csv', sep=';')
df_2019_2024 = pd.read_csv('./data/gold/departements_2019_2024.csv', sep=';')

# Filtrer les NaN dans les targets pour 2014
target_cols_list = ['pct_vote_extreme_gauche', 'pct_vote_gauche', 'pct_vote_centre',
                    'pct_vote_droite', 'pct_vote_extreme_droite']
df_2014 = df_2014.dropna(subset=target_cols_list)

# Séparer 2019 et 2024
df_2019 = df_2019_2024[df_2019_2024['annee'] == 2019].copy()
df_2024 = df_2019_2024[df_2019_2024['annee'] == 2024].copy()

print(f"    2014 : {len(df_2014)} lignes")
print(f"    2019 : {len(df_2019)} lignes")
print(f"    2024 : {len(df_2024)} lignes")
print(f"    → Train (2014+2019) : {len(df_2014) + len(df_2019)} lignes")
print(f"    → Test (2024)       : {len(df_2024)} lignes")

# ============================================================================
# HARMONISATION DES COLONNES
# ============================================================================
print("\n[2] HARMONISATION DES COLONNES...")

# Renommer pct_pop_65plus en pct_pop_64plus dans 2014 (c'est la même chose)
if 'pct_pop_65plus' in df_2014.columns:
    df_2014.rename(columns={'pct_pop_65plus': 'pct_pop_64plus'}, inplace=True)

# Colonnes à exclure
ignored_cols = ['dep_code', 'dep_libelle', 'annee', 'nuance_liste_gagnante',
                'pct_voix_liste_gagnante', 'pct_personnes_rsa',  # RSA absent en 2014
                'taux_crimes_pour_mille', 'taux_crimes_pour_mille_2016'] + target_cols_list

# Features communes
all_cols = set(df_2014.columns) & set(df_2019.columns) & set(df_2024.columns)
common_features = sorted([col for col in all_cols if col not in ignored_cols])

print(f"    Features communes : {len(common_features)}")
for feat in common_features:
    print(f"      • {feat}")

# ============================================================================
# PRÉPARATION DES DONNÉES
# ============================================================================
print("\n[3] PRÉPARATION DES DONNÉES...")

# Combiner 2014 et 2019 pour le train
df_train = pd.concat([df_2014, df_2019], ignore_index=True)

# Extraction des features
X_train = df_train[common_features].fillna(df_train[common_features].mean())
X_2024 = df_2024[common_features].fillna(df_2024[common_features].mean())

# Normalisation
scaler = StandardScaler()
X_train_scaled = pd.DataFrame(scaler.fit_transform(X_train),
                               columns=common_features, index=X_train.index)
X_2024_scaled = pd.DataFrame(scaler.transform(X_2024),
                              columns=common_features, index=X_2024.index)

print(f"    Train normalisé : {X_train_scaled.shape}")
print(f"    Test normalisé  : {X_2024_scaled.shape}")

# ============================================================================
# SPLIT TRAIN/VALIDATION
# ============================================================================
print("\n[4] SPLIT TRAIN/VALIDATION...")

X_tr, X_val = train_test_split(X_train_scaled, test_size=0.25, random_state=42)
train_idx, val_idx = X_tr.index, X_val.index

print(f"    Train      : {len(X_tr)} échantillons")
print(f"    Validation : {len(X_val)} échantillons")

# ============================================================================
# ENTRAÎNEMENT
# ============================================================================
print("\n[5] ENTRAÎNEMENT DES MODÈLES...")
print("\n    Algorithme : GradientBoosting avec régularisation")
print("    - learning_rate = 0.05 (apprentissage lent)")
print("    - max_depth = 5 (limite la complexité)")
print("    - subsample = 0.8 (80% des données par itération)")
print()

# Cibles
target_cols = {
    'EXTREME GAUCHE': 'pct_vote_extreme_gauche',
    'GAUCHE': 'pct_vote_gauche',
    'CENTRE': 'pct_vote_centre',
    'DROITE': 'pct_vote_droite',
    'EXTREME DROITE': 'pct_vote_extreme_droite',
}

all_models = {}
results = []

for nom_bord, target_col in target_cols.items():
    print(f"    {nom_bord}...")

    # Préparation des targets
    y_train = df_train.loc[train_idx, target_col]
    y_val = df_train.loc[val_idx, target_col]
    y_2024 = df_2024[target_col].values

    # Modèle avec RÉGULARISATION ULTRA-RENFORCÉE (Mode Anti-Overfit Maximum)
    model = GradientBoostingRegressor(
        n_estimators=150,
        learning_rate=0.03,
        max_depth=2,            # Arbres très superficiels (3 -> 2)
        min_samples_split=40,   # Demande énormément de données pour séparer
        min_samples_leaf=20,    # Demande énormément de données par feuille (10 -> 20)
        subsample=0.6,          # Seulement 60% des données par arbre
        random_state=42
    )

    # Validation croisée
    kfold = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X_tr, y_train, cv=kfold, scoring='r2')

    # Entraînement
    model.fit(X_tr, y_train)

    # Prédictions et métriques
    pred_train = model.predict(X_tr)
    pred_val = model.predict(X_val)
    pred_2024 = model.predict(X_2024_scaled)

    r2_train = r2_score(y_train, pred_train)
    mae_val = mean_absolute_error(y_val, pred_val)
    r2_val = r2_score(y_val, pred_val)
    mae_2024 = mean_absolute_error(y_2024, pred_2024)
    r2_2024 = r2_score(y_2024, pred_2024)

    # Détection overfitting
    overfit_gap = r2_train - r2_val

    # Sauvegarde
    all_models[nom_bord] = {
        'model': model,
        'scaler': scaler,
        'features': common_features
    }

    results.append({
        'Bord': nom_bord,
        'R2_Train': r2_train,
        'R2_Val': r2_val,
        'MAE_Val': mae_val,
        'R2_2024': r2_2024,
        'MAE_2024': mae_2024,
        'CV_R2_Mean': cv_scores.mean(),
        'CV_R2_Std': cv_scores.std(),
        'Overfit_Gap': overfit_gap
    })

# ============================================================================
# RÉSUMÉ
# ============================================================================
print("\n[6] RÉSUMÉ DES PERFORMANCES\n")

results_df = pd.DataFrame(results)

print("    ┌────────────────┬─────────┬─────────┬──────────┬──────────┬──────────┐")
print("    │ Bord Politique │ R² Train│ R² Val  │ MAE Val  │ R² 2024  │ Overfit  │")
print("    ├────────────────┼─────────┼─────────┼──────────┼──────────┼──────────┤")
for _, row in results_df.iterrows():
    overfit_symbol = "⚠️" if row['Overfit_Gap'] > 0.08 else "✓"
    print(f"    │ {row['Bord']:14s} │ {row['R2_Train']:7.3f} │ {row['R2_Val']:7.3f} │ {row['MAE_Val']:7.2f}% │ {row['R2_2024']:8.3f} │ {overfit_symbol:8s} │")
print("    └────────────────┴─────────┴─────────┴──────────┴──────────┴──────────┘")

print(f"\n    📊 MOYENNES :")
print(f"       Validation : MAE = {results_df['MAE_Val'].mean():.2f}% | R² = {results_df['R2_Val'].mean():.3f}")
print(f"       Test 2024  : MAE = {results_df['MAE_2024'].mean():.2f}% | R² = {results_df['R2_2024'].mean():.3f}")

# Analyse
print(f"\n    💡 ANALYSE :")
avg_r2_2024 = results_df['R2_2024'].mean()
if avg_r2_2024 > 0.5:
    print(f"       ✅ Excellente généralisation ! Le modèle prédit bien 2024.")
elif avg_r2_2024 > 0.2:
    print(f"       ✓ Bonne généralisation. Le modèle capture les tendances.")
elif avg_r2_2024 > 0:
    print(f"       ⚠️  Généralisation faible mais positive.")
else:
    print(f"       ❌ Généralisation négative. Le contexte a trop changé.")

# ============================================================================
# SAUVEGARDE
# ============================================================================
print("\n[7] SAUVEGARDE DES MODÈLES...\n")

os.makedirs('./models/saved', exist_ok=True)

for nom_bord, model_data in all_models.items():
    filename = f"./models/saved/model_{nom_bord.lower().replace(' ', '_')}.pkl"
    joblib.dump(model_data, filename)
    print(f"    ✓ {filename}")

# Sauvegarder aussi les résultats
results_df.to_csv('./models/resultats_entrainement.csv', index=False, sep=';')
print(f"    ✓ ./models/resultats_entrainement.csv")

# ============================================================================
# VISUALISATION
# ============================================================================
print("\n[8] GÉNÉRATION DES GRAPHIQUES...\n")

fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle('Performance des Modèles (Train: 2014+2019 → Test: 2024)',
             fontsize=14, fontweight='bold')

x = np.arange(len(results_df))
width = 0.35

# Graphique 1 : R²
axes[0].bar(x - width/2, results_df['R2_Val'], width,
            label='Validation (2014+2019)', alpha=0.8, color='steelblue')
axes[0].bar(x + width/2, results_df['R2_2024'], width,
            label='Test (2024)', alpha=0.8, color='coral')
axes[0].set_xlabel('Bord Politique')
axes[0].set_ylabel('Score R²')
axes[0].set_title('Qualité de l\'Ajustement')
axes[0].set_xticks(x)
axes[0].set_xticklabels(results_df['Bord'], rotation=45, ha='right')
axes[0].legend()
axes[0].grid(True, alpha=0.3, axis='y')
axes[0].axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.5)

# Graphique 2 : MAE
axes[1].bar(x - width/2, results_df['MAE_Val'], width,
            label='Validation (2014+2019)', alpha=0.8, color='steelblue')
axes[1].bar(x + width/2, results_df['MAE_2024'], width,
            label='Test (2024)', alpha=0.8, color='coral')
axes[1].set_xlabel('Bord Politique')
axes[1].set_ylabel('Erreur Moyenne (MAE en %)')
axes[1].set_title('Erreur de Prédiction')
axes[1].set_xticks(x)
axes[1].set_xticklabels(results_df['Bord'], rotation=45, ha='right')
axes[1].legend()
axes[1].grid(True, alpha=0.3, axis='y')

# Graphique 3 : Overfitting detection
colors = ['red' if gap > 0.15 else 'green' for gap in results_df['Overfit_Gap']]
axes[2].bar(x, results_df['Overfit_Gap'], color=colors, alpha=0.7)
axes[2].axhline(y=0.15, color='orange', linestyle='--', linewidth=2,
                label='Seuil overfitting (0.15)')
axes[2].set_xlabel('Bord Politique')
axes[2].set_ylabel('Écart R² (Train - Validation)')
axes[2].set_title('Détection Overfitting')
axes[2].set_xticks(x)
axes[2].set_xticklabels(results_df['Bord'], rotation=45, ha='right')
axes[2].legend()
axes[2].grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig('./models/performance.png', dpi=150, bbox_inches='tight')
print("    ✓ ./models/performance.png")

print("\n" + "=" * 80)
print("✅ ENTRAÎNEMENT TERMINÉ")
print("=" * 80)

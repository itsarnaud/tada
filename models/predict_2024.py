"""
PRÉDICTION SUR 2024 ET COMPARAISON AVEC LES RÉSULTATS RÉELS

Approche chronologique : Train (2014+2019) → Test (2024)
"""

import pandas as pd
import numpy as np
import joblib
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, r2_score

print("=" * 80)
print("PRÉDICTION 2024 ET COMPARAISON AVEC LES RÉSULTATS RÉELS")
print("=" * 80)

# ============================================================================
# CHARGEMENT
# ============================================================================
print("\n[1] CHARGEMENT DES DONNÉES ET DES MODÈLES...")

df_2019_2024 = pd.read_csv('./data/gold/departements_2019_2024.csv', sep=';')
df_2024 = df_2019_2024[df_2019_2024['annee'] == 2024].copy()

print(f"    Départements 2024 : {len(df_2024)}")

# Chargement des modèles
bords = ['EXTREME GAUCHE', 'GAUCHE', 'CENTRE', 'DROITE', 'EXTREME DROITE']
models = {}

for bord in bords:
    filename = f"./models/saved/model_{bord.lower().replace(' ', '_')}.pkl"
    models[bord] = joblib.load(filename)

print(f"    Modèles chargés : {len(models)}")

# ============================================================================
# PRÉPARATION
# ============================================================================
print("\n[2] PRÉPARATION DES FEATURES...")

scaler = models['EXTREME GAUCHE']['scaler']
features = models['EXTREME GAUCHE']['features']

X_2024 = df_2024[features].fillna(df_2024[features].mean())
X_2024_scaled = scaler.transform(X_2024)

print(f"    Features préparées : {len(features)}")

# ============================================================================
# PRÉDICTIONS
# ============================================================================
print("\n[3] GÉNÉRATION DES PRÉDICTIONS...\n")

predictions = {}
for bord, model_data in models.items():
    predictions[bord] = model_data['model'].predict(X_2024_scaled)
    print(f"    ✓ {bord}")

# Création du DataFrame de résultats
results_df = df_2024[['dep_code', 'dep_libelle']].copy()

target_cols = {
    'EXTREME GAUCHE': 'pct_vote_extreme_gauche',
    'GAUCHE': 'pct_vote_gauche',
    'CENTRE': 'pct_vote_centre',
    'DROITE': 'pct_vote_droite',
    'EXTREME DROITE': 'pct_vote_extreme_droite',
}

for bord, target_col in target_cols.items():
    results_df[f'{bord}_reel'] = df_2024[target_col]
    results_df[f'{bord}_predit'] = predictions[bord]
    results_df[f'{bord}_erreur'] = abs(results_df[f'{bord}_reel'] - results_df[f'{bord}_predit'])

# ============================================================================
# PERFORMANCES
# ============================================================================
print("\n[4] ANALYSE DES PERFORMANCES\n")

print("    ┌────────────────┬──────────┬───────┬──────────┬──────────┐")
print("    │ Bord Politique │ MAE      │ R²    │ Min Err  │ Max Err  │")
print("    ├────────────────┼──────────┼───────┼──────────┼──────────┤")

performance_summary = []

for bord, target_col in target_cols.items():
    y_true = df_2024[target_col]
    y_pred = predictions[bord]

    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    min_err = results_df[f'{bord}_erreur'].min()
    max_err = results_df[f'{bord}_erreur'].max()

    performance_summary.append({
        'Bord': bord,
        'MAE': mae,
        'R2': r2,
        'Min_Err': min_err,
        'Max_Err': max_err
    })

    print(f"    │ {bord:14s} │ {mae:7.2f}% │ {r2:5.2f}  │ {min_err:7.2f}% │ {max_err:7.2f}% │")

print("    └────────────────┴──────────┴───────┴──────────┴──────────┘")

# Moyennes
perf_df = pd.DataFrame(performance_summary)
print(f"\n    📊 MOYENNES GLOBALES :")
print(f"       MAE  : {perf_df['MAE'].mean():.2f}%")
print(f"       R²   : {perf_df['R2'].mean():.3f}")

# Interprétation
print(f"\n    💡 INTERPRÉTATION :")
if perf_df['R2'].mean() > 0.5:
    print(f"       ✅ Excellentes prédictions ! Le modèle généralise très bien.")
elif perf_df['R2'].mean() > 0.2:
    print(f"       ✓ Bonnes prédictions. Le modèle capture les tendances.")
elif perf_df['R2'].mean() > 0:
    print(f"       ⚠️  Prédictions moyennes. Il y a une certaine généralisation.")
else:
    print(f"       ❌ Prédictions faibles. Le contexte a beaucoup changé.")

# ============================================================================
# ANALYSE DES ERREURS (PIRES ET MEILLEURES)
# ============================================================================
print("\n[5] ANALYSE DES PRÉDICTIONS PAR DÉPARTEMENT\n")

for bord in bords:
    print(f"    {'='*10} {bord} {'='*10}")

    # TOP 5 ERREURS (Pires)
    print(f"    PIRES PRÉDICTIONS (Top 5 erreurs) :")
    top_errors = results_df.nlargest(5, f'{bord}_erreur')[
        ['dep_libelle', f'{bord}_reel', f'{bord}_predit', f'{bord}_erreur']
    ]
    for _, row in top_errors.iterrows():
        print(f"        {row['dep_libelle']:25s} (réel: {row[f'{bord}_reel']:5.1f}% | "
              f"prédit: {row[f'{bord}_predit']:5.1f}% | erreur: {row[f'{bord}_erreur']:5.1f}%)")

    # TOP 5 SUCCÈS (Meilleures)
    print(f"\n    MEILLEURES PRÉDICTIONS (Plus petites erreurs) :")
    top_success = results_df.nsmallest(5, f'{bord}_erreur')[
        ['dep_libelle', f'{bord}_reel', f'{bord}_predit', f'{bord}_erreur']
    ]
    for _, row in top_success.iterrows():
        print(f"        {row['dep_libelle']:25s} (réel: {row[f'{bord}_reel']:5.1f}% | "
              f"prédit: {row[f'{bord}_predit']:5.1f}% | erreur: {row[f'{bord}_erreur']:5.1f}%)")
    print("\n")

# ============================================================================
# SAUVEGARDE
# ============================================================================
print("[6] SAUVEGARDE...\n")

results_df.to_csv('./models/predictions_2024.csv', index=False, sep=';')
print("    ✓ ./models/predictions_2024.csv")

# ============================================================================
# VISUALISATION
# ============================================================================
print("\n[7] GÉNÉRATION DES GRAPHIQUES...\n")

# Graphique : Scatter plots réel vs prédit
fig, axes = plt.subplots(2, 3, figsize=(16, 10))
fig.suptitle('Prédictions vs Résultats Réels 2024\n(Entraîné sur 2014+2019)',
             fontsize=16, fontweight='bold')

for idx, (bord, target_col) in enumerate(target_cols.items()):
    row = idx // 3
    col = idx % 3

    if idx < 5:
        ax = axes[row, col]

        y_true = df_2024[target_col].values
        y_pred = predictions[bord]

        ax.scatter(y_true, y_pred, alpha=0.6, s=50, edgecolors='black', linewidth=0.5)

        # Ligne de référence
        min_val = min(y_true.min(), y_pred.min())
        max_val = max(y_true.max(), y_pred.max())
        ax.plot([min_val, max_val], [min_val, max_val], 'r--', lw=2, label='Prédiction parfaite')

        mae = mean_absolute_error(y_true, y_pred)
        r2 = r2_score(y_true, y_pred)

        ax.set_xlabel('Pourcentage Réel (%)', fontsize=11)
        ax.set_ylabel('Pourcentage Prédit (%)', fontsize=11)
        ax.set_title(f'{bord}\nMAE: {mae:.2f}% | R²: {r2:.3f}', fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

        # Ajouter ligne des identités
        ax.plot([min_val, max_val], [min_val, max_val], 'k-', alpha=0.2, lw=1)

axes[1, 2].remove()

plt.tight_layout()
plt.savefig('./models/predictions_2024.png', dpi=150, bbox_inches='tight')
print("    ✓ ./models/predictions_2024.png")

# Graphique 2 : Comparaison des performances
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Analyse des Performances sur 2024', fontsize=14, fontweight='bold')

# MAE par bord
colors_mae = ['green' if mae < 5 else 'orange' if mae < 10 else 'red'
              for mae in perf_df['MAE']]
ax1.bar(range(len(perf_df)), perf_df['MAE'], color=colors_mae, alpha=0.7)
ax1.set_xlabel('Bord Politique')
ax1.set_ylabel('MAE (points de %)')
ax1.set_title('Erreur Moyenne par Bord')
ax1.set_xticks(range(len(perf_df)))
ax1.set_xticklabels(perf_df['Bord'], rotation=45, ha='right')
ax1.grid(True, alpha=0.3, axis='y')
ax1.axhline(y=5, color='orange', linestyle='--', linewidth=1, label='Seuil acceptable (5%)')
ax1.legend()

# R² par bord
colors_r2 = ['green' if r2 > 0.5 else 'orange' if r2 > 0.2 else 'red'
             for r2 in perf_df['R2']]
ax2.bar(range(len(perf_df)), perf_df['R2'], color=colors_r2, alpha=0.7)
ax2.set_xlabel('Bord Politique')
ax2.set_ylabel('Score R²')
ax2.set_title('Qualité des Prédictions par Bord')
ax2.set_xticks(range(len(perf_df)))
ax2.set_xticklabels(perf_df['Bord'], rotation=45, ha='right')
ax2.grid(True, alpha=0.3, axis='y')
ax2.axhline(y=0, color='red', linestyle='--', linewidth=1, alpha=0.5)
ax2.axhline(y=0.5, color='green', linestyle='--', linewidth=1, label='Excellent (0.5)')
ax2.legend()

plt.tight_layout()
plt.savefig('./models/performance_par_bord_2024.png', dpi=150, bbox_inches='tight')
print("    ✓ ./models/performance_par_bord_2024.png")

print("\n" + "=" * 80)
print("✅ ANALYSE TERMINÉE")
print("=" * 80)
print(f"\n💡 Les modèles entraînés sur 2014+2019 ont un R² moyen de {perf_df['R2'].mean():.3f} sur 2024")
print(f"   C'est {'excellent' if perf_df['R2'].mean() > 0.5 else 'bon' if perf_df['R2'].mean() > 0.2 else 'moyen' if perf_df['R2'].mean() > 0 else 'faible'} !")

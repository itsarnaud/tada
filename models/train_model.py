import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble        import RandomForestRegressor
from sklearn.metrics         import mean_absolute_error, r2_score
import matplotlib.pyplot as plt

df = pd.read_csv('./data/gold/departements_2019_2024.csv', sep=';')

ignored_cols = ['dep_code', 'dep_libelle', 'annee', 'nuance_liste_gagnante', 'pct_voix_liste_gagnante',
                'pct_vote_extreme_gauche', 'pct_vote_gauche', 'pct_vote_centre',
                'pct_vote_droite', 'pct_vote_extreme_droite']

X = df.drop(columns=ignored_cols)

targets = {
    'EXTREME GAUCHE': df['pct_vote_extreme_gauche'],
    'GAUCHE':         df['pct_vote_gauche'],
    'CENTRE':         df['pct_vote_centre'],
    'DROITE':         df['pct_vote_droite'],
    'EXTREME DROITE': df['pct_vote_extreme_droite'],
}

X_train, X_test = train_test_split(X, test_size=0.2, random_state=42)
train_idx = X_train.index
test_idx = X_test.index

def afficher_importance_criteres(modele, nom_modele):
  importances = pd.DataFrame({
    'Critère': X.columns,
    'Importance (%)': modele.feature_importances_ * 100
  })
  importances = importances.sort_values(by='Importance (%)', ascending=False)
  print(f"Top 5 des critères qui influencent le vote {nom_modele} :")
  print(importances.head())
  print("-" * 40)

modeles = {}
for nom_bord, y in targets.items():
    print(f"--- ENTRAÎNEMENT MODÈLE {nom_bord} ---")
    y_train = y.loc[train_idx]
    y_test = y.loc[test_idx]
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    r2 = r2_score(y_test, pred)
    print(f"Erreur Moyenne (MAE) : {mae:.2f} points de %")
    print(f"Fiabilité (R2) : {r2:.2f}\n")
    afficher_importance_criteres(model, nom_bord)
    modeles[nom_bord] = model

# Affiche la corrélation (+ c'est proche de 1, plus ça monte ensemble. -1 c'est l'inverse)
for nom_bord, col in [
    ('EXTREME GAUCHE', 'pct_vote_extreme_gauche'),
    ('GAUCHE',         'pct_vote_gauche'),
    ('CENTRE',         'pct_vote_centre'),
    ('DROITE',         'pct_vote_droite'),
    ('EXTREME DROITE', 'pct_vote_extreme_droite'),
]:
    print(f"\nCorrélation avec le vote {nom_bord} :")
    print(df.corr(numeric_only=True)[col].sort_values(ascending=False))

print("\n--- CRÉATION DU GRAPHIQUE DE PERFORMANCE ---")

liste_nb_arbres = range(1, 105, 5)
erreurs_gauche = []
r2_gauche = []

for nb_arbres in liste_nb_arbres:
    modele_temp = RandomForestRegressor(n_estimators=nb_arbres, random_state=42)
    modele_temp.fit(X_train, y_train_g)
    
    pred_temp = modele_temp.predict(X_test)
    
    erreur = mean_absolute_error(y_test_g, pred_temp)
    score_fiabilite = r2_score(y_test_g, pred_temp)
    
    erreurs_gauche.append(erreur)
    r2_gauche.append(score_fiabilite)

# --- Dessin du graphique avec deux axes Y ---
fig, ax1 = plt.subplots(figsize=(10, 6))

# 1ère courbe : L'Erreur (MAE)
color_mae = 'tab:blue'
ax1.set_xlabel("Nombre d'arbres dans la Forêt (n_estimators)", fontsize=12)
ax1.set_ylabel("Marge d'erreur (MAE) en points de %", color=color_mae, fontsize=12)
ligne1 = ax1.plot(liste_nb_arbres, erreurs_gauche, marker='o', color=color_mae, linewidth=2, label='Erreur MAE')
ax1.tick_params(axis='y', labelcolor=color_mae)
ax1.grid(True, linestyle=':', alpha=0.7)

# 2ème courbe : La Fiabilité (R2) sur un second axe à droite
ax2 = ax1.twinx()  
color_r2 = 'tab:green'
ax2.set_ylabel("Fiabilité du modèle (Score R²)", color=color_r2, fontsize=12)
ligne2 = ax2.plot(liste_nb_arbres, r2_gauche, marker='s', color=color_r2, linewidth=2, label='Fiabilité R²')
ax2.tick_params(axis='y', labelcolor=color_r2)

ligne_choix = ax1.axvline(x=100, color='red', linestyle='--', label='Choix final (100 arbres)')

lignes = ligne1 + ligne2 + [ligne_choix]
labels = [l.get_label() for l in lignes]
ax1.legend(lignes, labels, loc='center right')

plt.title("Évolution des performances du modèle en fonction de la taille de la forêt", fontsize=14)
fig.tight_layout()

print("Affichage du premier graphique... (Fermez la fenêtre du graphique pour voir le suivant)")
plt.show()


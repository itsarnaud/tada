import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble        import RandomForestRegressor
from sklearn.metrics         import mean_absolute_error, r2_score
import matplotlib.pyplot as plt

df = pd.read_csv('./data/silver/departements_2019_2024.csv', sep=';')

ignored_cols = ['dep_code', 'dep_libelle', 'annee', 'nuance_liste_gagnante', 'pct_voix_liste_gagnante', 'pct_vote_droite', 'pct_vote_gauche']

X = df.drop(columns=ignored_cols)

y_gauche = df['pct_vote_gauche']
y_droite = df['pct_vote_droite']

X_train, X_test, y_train_g, y_test_g, y_train_d, y_test_d = train_test_split(
  X, y_gauche, y_droite, test_size=0.2, random_state=42
)

# --- MODÈLE GAUCHE ---
print("--- ENTRAÎNEMENT MODÈLE GAUCHE ---")
modele_gauche = RandomForestRegressor(n_estimators=100, random_state=42)
modele_gauche.fit(X_train, y_train_g)

pred_gauche = modele_gauche.predict(X_test)
mae_g = mean_absolute_error(y_test_g, pred_gauche)
r2_g = r2_score(y_test_g, pred_gauche)
print(f"Erreur Moyenne (MAE) : {mae_g:.2f} points de %")
print(f"Fiabilité (R2) : {r2_g:.2f}\n")

# --- MODÈLE DROITE ---
print("--- ENTRAÎNEMENT MODÈLE DROITE ---")
modele_droite = RandomForestRegressor(n_estimators=100, random_state=42)
modele_droite.fit(X_train, y_train_d)

pred_droite = modele_droite.predict(X_test)
mae_d = mean_absolute_error(y_test_d, pred_droite)
r2_d = r2_score(y_test_d, pred_droite)
print(f"Erreur Moyenne (MAE) : {mae_d:.2f} points de %")
print(f"Fiabilité (R2) : {r2_d:.2f}\n")

def afficher_importance_criteres(modele, nom_modele):
  importances = pd.DataFrame({
    'Critère': X.columns,
    'Importance (%)': modele.feature_importances_ * 100
  })
  importances = importances.sort_values(by='Importance (%)', ascending=False)
  print(f"Top 5 des critères qui influencent le vote à {nom_modele} :")
  print(importances.head())
  print("-" * 40)

afficher_importance_criteres(modele_gauche, "GAUCHE")
afficher_importance_criteres(modele_droite, "DROITE")

# Affiche la corrélation (+ c'est proche de 1, plus ça monte ensemble. -1 c'est l'inverse)
print("\nCorrélation avec le vote à Gauche :")
print(df.corr(numeric_only=True)['pct_vote_gauche'].sort_values(ascending=False))

print("\nCorrélation avec le vote à Droite :")
print(df.corr(numeric_only=True)['pct_vote_droite'].sort_values(ascending=False))

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


import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble        import RandomForestRegressor
from sklearn.metrics         import mean_absolute_error, r2_score

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

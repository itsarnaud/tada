from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    confusion_matrix,
    f1_score,
)

print("=" * 80)
print("PRÉDICTION BORD POLITIQUE - TEST 2024 (5 CLASSES)")
print("=" * 80)

BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR / "saved"
OUTPUT_DIR = BASE_DIR / "outputs"

print("\n[1] CHARGEMENT...")
bundle = joblib.load(MODEL_DIR / "model_bord_politique.pkl")
model = bundle["model"]
imputer = bundle["imputer"]
scaler = bundle["scaler"]
features = bundle["features"]
labels = bundle["labels"]

df = pd.read_csv("./data/gold/departements_2019_2024.csv", sep=";")
df_2024 = df[
    (df["annee"] == 2024) & (df["nuance_liste_gagnante"] != "NON COMMUNIQUE")
].copy()

print(f"    Départements 2024 : {len(df_2024)}")
print(f"    Features : {len(features)}")

print("\n[2] PRÉPARATION...")
X = df_2024[features].copy()
X_imp = pd.DataFrame(imputer.transform(X), columns=features, index=X.index)
X_scaled = pd.DataFrame(scaler.transform(X_imp), columns=features, index=X.index)
y_true = df_2024["nuance_liste_gagnante"]

print("\n[3] PRÉDICTIONS...")
y_pred = model.predict(X_scaled)

acc = accuracy_score(y_true, y_pred)
bal_acc = balanced_accuracy_score(y_true, y_pred)
f1_macro = f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)

print(f"    ACC      : {acc:.3f}")
print(f"    BAL_ACC  : {bal_acc:.3f}")
print(f"    F1_macro : {f1_macro:.3f}")

print("\n[4] MATRICE DE CONFUSION...")
cm = confusion_matrix(y_true, y_pred, labels=labels)
cm_df = pd.DataFrame(
    cm, index=[f"reel_{c}" for c in labels], columns=[f"pred_{c}" for c in labels]
)
print(cm_df.to_string())

print("\n[5] SAUVEGARDE...")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

results = df_2024[["dep_code", "dep_libelle", "nuance_liste_gagnante"]].copy()
results["bord_predit"] = y_pred
results["correct"] = results["nuance_liste_gagnante"] == results["bord_predit"]
results.to_csv(OUTPUT_DIR / "predictions_bord_2024.csv", index=False, sep=";")
cm_df.to_csv(OUTPUT_DIR / "confusion_matrix_bord_2024.csv", sep=";")

print(f"    ✓ {(OUTPUT_DIR / 'predictions_bord_2024.csv').as_posix()}")
print(f"    ✓ {(OUTPUT_DIR / 'confusion_matrix_bord_2024.csv').as_posix()}")

print("\n" + "=" * 80)
print("✅ PRÉDICTION TERMINÉE")
print("=" * 80)

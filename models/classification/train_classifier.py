"""
ENTRAÎNEMENT D'UN MODÈLE DE CLASSIFICATION MULTI-CLASSES (5 BORDS)

Validation temporelle stricte :
- Train : 2014
- Validation : 2019
- Test : 2024
"""

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, balanced_accuracy_score, confusion_matrix, f1_score
from sklearn.preprocessing import StandardScaler


print("=" * 80)
print("CLASSIFICATION MULTI-CLASSES (5 BORDS) - VALIDATION TEMPORELLE")
print("Train : 2014  →  Validation : 2019  →  Test : 2024")
print("=" * 80)

BASE_DIR = Path(__file__).resolve().parent
MODEL_DIR = BASE_DIR / "saved"
OUTPUT_DIR = BASE_DIR / "outputs"

CLASSES = ["EXTREME GAUCHE", "GAUCHE", "CENTRE", "DROITE", "EXTREME DROITE"]


def compute_metrics(y_true: pd.Series, y_pred: np.ndarray, labels: list[str]) -> dict:
    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "balanced_accuracy": balanced_accuracy_score(y_true, y_pred),
        "f1_macro": f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0),
    }


def to_cm_df(y_true: pd.Series, y_pred: np.ndarray, labels: list[str]) -> pd.DataFrame:
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    return pd.DataFrame(
        cm,
        index=[f"reel_{label}" for label in labels],
        columns=[f"pred_{label}" for label in labels],
    )


print("\n[1] CHARGEMENT DES DONNÉES...")
df_2014 = pd.read_csv("./data/gold/departements_2014.csv", sep=";")
df_2019_2024 = pd.read_csv("./data/gold/departements_2019_2024.csv", sep=";")

if "pct_pop_65plus" in df_2014.columns:
    df_2014 = df_2014.rename(columns={"pct_pop_65plus": "pct_pop_64plus"})

df_2019 = df_2019_2024[df_2019_2024["annee"] == 2019].copy()
df_2024 = df_2019_2024[df_2019_2024["annee"] == 2024].copy()

for df in (df_2014, df_2019, df_2024):
    df.dropna(subset=["nuance_liste_gagnante"], inplace=True)
    df = df[df["nuance_liste_gagnante"] != "NON COMMUNIQUE"]

df_2014 = df_2014[df_2014["nuance_liste_gagnante"] != "NON COMMUNIQUE"].copy()
df_2019 = df_2019[df_2019["nuance_liste_gagnante"] != "NON COMMUNIQUE"].copy()
df_2024 = df_2024[df_2024["nuance_liste_gagnante"] != "NON COMMUNIQUE"].copy()

print(f"    2014 : {len(df_2014)}")
print(f"    2019 : {len(df_2019)}")
print(f"    2024 : {len(df_2024)}")
print(f"    Distribution 2014:\n{df_2014['nuance_liste_gagnante'].value_counts().to_string()}")
print(f"    Distribution 2019:\n{df_2019['nuance_liste_gagnante'].value_counts().to_string()}")
print(f"    Distribution 2024:\n{df_2024['nuance_liste_gagnante'].value_counts().to_string()}")

print("\n[2] SÉLECTION DES FEATURES...")
target_pct_cols = [
    "pct_vote_extreme_gauche",
    "pct_vote_gauche",
    "pct_vote_centre",
    "pct_vote_droite",
    "pct_vote_extreme_droite",
]
ignored_cols = {
    "dep_code",
    "dep_libelle",
    "annee",
    "nuance_liste_gagnante",
    "liste_gagnante",
    "pct_voix_liste_gagnante",
    "pct_personnes_rsa",
    "taux_crimes_pour_mille",
    "taux_crimes_pour_mille_2016",
    *target_pct_cols,
}

all_cols = set(df_2014.columns) & set(df_2019.columns) & set(df_2024.columns)
features = sorted([col for col in all_cols if col not in ignored_cols])
print(f"    Features : {len(features)}")

print("\n[3] PRÉPARATION...")
X_train_raw = df_2014[features].copy()
y_train = df_2014["nuance_liste_gagnante"].copy()

X_val_raw = df_2019[features].copy()
y_val = df_2019["nuance_liste_gagnante"].copy()

X_test_raw = df_2024[features].copy()
y_test = df_2024["nuance_liste_gagnante"].copy()

imputer = SimpleImputer(strategy="median")
X_train_imp = pd.DataFrame(imputer.fit_transform(X_train_raw), columns=features, index=X_train_raw.index)
X_val_imp = pd.DataFrame(imputer.transform(X_val_raw), columns=features, index=X_val_raw.index)
X_test_imp = pd.DataFrame(imputer.transform(X_test_raw), columns=features, index=X_test_raw.index)

scaler = StandardScaler()
X_train = pd.DataFrame(scaler.fit_transform(X_train_imp), columns=features, index=X_train_imp.index)
X_val = pd.DataFrame(scaler.transform(X_val_imp), columns=features, index=X_val_imp.index)
X_test = pd.DataFrame(scaler.transform(X_test_imp), columns=features, index=X_test_imp.index)

print("\n[4] ENTRAÎNEMENT ET SÉLECTION (VAL 2019)...")
candidate_depth = [4, 6, 8, 10]
best = None

for depth in candidate_depth:
    clf = RandomForestClassifier(
        n_estimators=500,
        max_depth=depth,
        min_samples_leaf=2,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    clf.fit(X_train, y_train)
    pred_val = clf.predict(X_val)
    val_scores = compute_metrics(y_val, pred_val, CLASSES)
    print(
        f"    max_depth={depth:<2} -> VAL ACC={val_scores['accuracy']:.3f} | "
        f"BAL_ACC={val_scores['balanced_accuracy']:.3f} | F1_macro={val_scores['f1_macro']:.3f}"
    )
    if best is None or val_scores["balanced_accuracy"] > best["val_bal_acc"]:
        best = {
            "depth": depth,
            "model": clf,
            "val_scores": val_scores,
            "val_bal_acc": val_scores["balanced_accuracy"],
        }

print(f"\n    Meilleur max_depth (sur BAL_ACC 2019): {best['depth']}")

print("\n[5] TEST 2024...")
best_model = best["model"]
pred_test = best_model.predict(X_test)
test_scores = compute_metrics(y_test, pred_test, CLASSES)

majority_class = y_train.value_counts().idxmax()
baseline_pred_test = np.full(shape=len(y_test), fill_value=majority_class)
baseline_scores = compute_metrics(y_test, baseline_pred_test, CLASSES)

print(
    f"    Modèle : ACC={test_scores['accuracy']:.3f} | "
    f"BAL_ACC={test_scores['balanced_accuracy']:.3f} | F1_macro={test_scores['f1_macro']:.3f}"
)
print(
    f"    Baseline ({majority_class}) : ACC={baseline_scores['accuracy']:.3f} | "
    f"BAL_ACC={baseline_scores['balanced_accuracy']:.3f} | F1_macro={baseline_scores['f1_macro']:.3f}"
)

print("\n[6] SAUVEGARDE...")
MODEL_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

cm_val_df = to_cm_df(y_val, best_model.predict(X_val), CLASSES)
cm_test_df = to_cm_df(y_test, pred_test, CLASSES)

metrics_df = pd.DataFrame(
    [
        {
            "task": "multiclass_5_bords",
            "validation_year": 2019,
            "test_year": 2024,
            "best_max_depth": best["depth"],
            "val_accuracy": best["val_scores"]["accuracy"],
            "val_balanced_accuracy": best["val_scores"]["balanced_accuracy"],
            "val_f1_macro": best["val_scores"]["f1_macro"],
            "test_accuracy": test_scores["accuracy"],
            "test_balanced_accuracy": test_scores["balanced_accuracy"],
            "test_f1_macro": test_scores["f1_macro"],
            "baseline_majority_class": majority_class,
            "baseline_test_accuracy": baseline_scores["accuracy"],
            "baseline_test_balanced_accuracy": baseline_scores["balanced_accuracy"],
            "baseline_test_f1_macro": baseline_scores["f1_macro"],
        }
    ]
)
metrics_df.to_csv(OUTPUT_DIR / "resultats_classification_bord.csv", index=False, sep=";")

predictions_df = df_2024[["dep_code", "dep_libelle", "nuance_liste_gagnante"]].copy()
predictions_df["bord_predit"] = pred_test
predictions_df["correct"] = predictions_df["nuance_liste_gagnante"] == predictions_df["bord_predit"]
predictions_df.to_csv(OUTPUT_DIR / "predictions_bord_2024.csv", index=False, sep=";")

cm_val_df.to_csv(OUTPUT_DIR / "confusion_matrix_bord_2019.csv", sep=";")
cm_test_df.to_csv(OUTPUT_DIR / "confusion_matrix_bord_2024.csv", sep=";")

bundle = {
    "model": best_model,
    "imputer": imputer,
    "scaler": scaler,
    "features": features,
    "labels": CLASSES,
}
joblib.dump(bundle, MODEL_DIR / "model_bord_politique.pkl")

print(f"    ✓ {(MODEL_DIR / 'model_bord_politique.pkl').as_posix()}")
print(f"    ✓ {(OUTPUT_DIR / 'resultats_classification_bord.csv').as_posix()}")
print(f"    ✓ {(OUTPUT_DIR / 'predictions_bord_2024.csv').as_posix()}")
print(f"    ✓ {(OUTPUT_DIR / 'confusion_matrix_bord_2019.csv').as_posix()}")
print(f"    ✓ {(OUTPUT_DIR / 'confusion_matrix_bord_2024.csv').as_posix()}")

print("\n" + "=" * 80)
print("✅ CLASSIFICATION MULTI-CLASSES TERMINÉE")
print("=" * 80)

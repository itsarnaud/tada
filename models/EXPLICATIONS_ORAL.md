# 🎤 Explications pour l'Oral - VERSION FINALE

## ⚠️ DÉCOUVERTE IMPORTANTE

**On a testé les DEUX sens de prédiction, et les deux échouent !**

| Approche | Train | Test | R² moyen | Conclusion |
|----------|-------|------|----------|------------|
| Inversée (mal) | 2019+2024 | 2014 | **-7.6** | ❌ Très mauvais |
| Chronologique (bien) | 2014+2019 | 2024 | **-2.3** | ❌ Mauvais |

**→ Le problème n'est PAS l'ordre chronologique !**  
**→ Le problème : Les données socio-économiques ne capturent pas les changements politiques sur 5-10 ans**

---

## 1. C'est quoi l'overfitting ?

### Analogie simple

**Apprendre par cœur vs Comprendre**

```
📚 OVERFITTING = Apprendre par cœur
Tu connais toutes les réponses des anciens exams
→ 100% sur les anciens exams
→ 30% le jour J

🧠 PAS D'OVERFITTING = Comprendre
Tu comprends les concepts
→ 80% sur les anciens exams
→ 75% le jour J
```

### Dans notre cas

Nos modèles ont un **léger overfitting** (écart train-validation) mais ce n'est PAS le vrai problème.

Le vrai problème : **Généralisation temporelle impossible** sur 5-10 ans

---

## 2. Les résultats (approche chronologique correcte)

### Train : 2014+2019 (197 lignes) → Test : 2024 (101 lignes)

| Bord | MAE Validation | R² Validation | MAE Test 2024 | R² Test 2024 |
|------|----------------|---------------|---------------|--------------|
| EXTREME GAUCHE | 0.27% | 0.38 | 0.37% | -0.28 |
| **GAUCHE** | 3.72% | 0.31 | 4.99% | **0.17** ✓ |
| CENTRE | 3.71% | 0.58 | 6.10% | -5.51 |
| DROITE | 5.48% | 0.35 | 4.17% | -3.53 |
| EXTREME DROITE | 3.96% | 0.52 | 13.03% | -2.41 |
| **MOYENNE** | **3.43%** | **0.43** | **5.73%** | **-2.31** |

**Interprétation :**
- ✓ Validation OK (R²=0.43, MAE=3.43%)
- ✗ Test 2024 KO (R²=-2.31, MAE=5.73%)
- Seule la GAUCHE généralise un peu (R²=0.17)

---

## 3. Pourquoi ça ne marche pas ?

### Le contexte politique a trop changé

**2014 :**
- FN émergeant (Marine Le Pen)
- PS au pouvoir (Hollande)
- Pas de Macron
- Centre faible (MoDem)

**2019 :**
- Gilets Jaunes
- RN confirmé
- LREM dominant (Macron élu en 2017)
- Effondrement PS

**2024 :**
- RN encore plus fort
- Dissolution surprise
- Fragmentation totale
- Nouveau Front Populaire

### Les features socio-économiques ne capturent PAS :
- ❌ Les changements d'alliances
- ❌ Les événements politiques majeurs
- ❌ Les scandales, crises
- ❌ Le momentum électoral
- ❌ La popularité des leaders

---

## 4. Ce qu'on a fait techniquement

### Algorithme : GradientBoosting

**C'est quoi ?**
- Ensemble d'arbres de décision séquentiels
- Chaque arbre corrige les erreurs du précédent
- Régularisation intégrée

**Pourquoi ?**
- Excellent avec peu de données
- Moins sensible à l'overfitting que RandomForest
- Performant sur données tabulaires

### Features (15 variables)

```
pop_total
pct_policiers_municipaux
pct_foyers_imposes              ← La plus importante
revenu_fiscal_moyen_par_foyer
ratio_actifs_retraites
pct_individuel_pur
pct_individuel_groupe
pct_collectif
pct_residence
pct_pop_0_14
pct_pop_15_29
pct_pop_30_64
pct_pop_64plus
pct_demandeurs_emploi
pct_naissances
```

### Anti-overfitting appliqué

1. ✅ **Validation croisée 5-fold**
2. ✅ **Régularisation** (max_depth=5, learning_rate=0.05, subsample=0.8)
3. ✅ **Split train/validation** (75/25)
4. ✅ **Normalisation** (StandardScaler)
5. ✅ **Test sur année séparée** (2024)

**Résultat : L'overfitting est maîtrisé, mais la généralisation temporelle échoue**

---

## 5. La vraie conclusion du POC

### Message principal

> "Nous avons créé des modèles robustes qui fonctionnent bien sur la validation (R²=0.43). Nous avons testé les deux sens de prédiction : 2019-2024→2014 ET 2014-2019→2024. **Dans les deux cas, les R² sont négatifs.** Conclusion : Les données socio-économiques seules ne permettent PAS de prédire les élections européennes sur 5-10 ans, car le contexte politique change trop rapidement."

### Ce qui marche

✅ Techniques ML maîtrisées (validation croisée, régularisation, etc.)  
✅ Pas d'overfitting classique (bon équilibre train-validation)  
✅ GAUCHE est le seul bord un peu prédictible (vote populaire stable)  
✅ Analyse critique et tests dans les deux sens

### Ce qui ne marche pas

❌ Prédiction cross-temporelle sur 5-10 ans  
❌ R² négatifs dans les deux sens  
❌ Features socio-économiques insuffisantes  
❌ Contexte politique trop volatile

---

## 6. Améliorations possibles

### Court terme (si on avait plus de temps)

1. **Prédire N+1 au lieu de N+5**
   - Entraîner sur 2019 → Prédire 2024 (5 ans OK)
   - Entraîner sur 2024 → Prédire 2029 (usage réel)

2. **Ajouter les résultats précédents**
   - Feature : "score_gauche_election_precedente"
   - Feature : "tendance" (évolution)
   - Feature : "participation"

3. **Features de momentum**
   - Différence entre N et N-5
   - Vitesse de changement

### Long terme (vrai projet)

1. **Plus de données historiques**
   - 2009, 2004, 1999... → Plus de patterns

2. **Modèles de séries temporelles**
   - ARIMA, Prophet
   - Spécialisés pour l'évolution temporelle

3. **Features contextuelles**
   - Popularité du président
   - Taux de chômage national
   - Sentiment Twitter/médias
   - Événements majeurs codés

4. **Modèles par cluster**
   - Grouper les départements similaires
   - Modèle spécialisé par cluster

---

## 7. Pour l'oral - Questions/Réponses

### Q: Pourquoi R² négatif ?

**R:** Le modèle fait pire qu'une simple moyenne. Cela arrive quand le contexte est trop différent entre train et test. Ici, le paysage politique de 2024 est très différent de 2014-2019 (montée du RN, effondrement du PS, etc.).

### Q: C'est de l'overfitting ?

**R:** Non ! L'overfitting se voit entre train et validation. Ici, on a R² validation = 0.43, c'est correct. Le problème est la généralisation temporelle : prédire 5-10 ans dans le futur/passé est impossible avec seulement des données socio-économiques.

### Q: Vous avez essayé dans l'autre sens ?

**R:** OUI ! On a testé 2019-2024→2014 (R²=-7.6) ET 2014-2019→2024 (R²=-2.3). Les deux échouent. La chronologie n'est pas le seul problème.

### Q: Pourquoi la GAUCHE marche mieux ?

**R:** Le vote de gauche est plus corrélé aux données socio-économiques (chômage, revenus, population). C'est un vote "de classe" plus stable. L'extrême droite, elle, est plus volatile et liée à des événements politiques ponctuels.

### Q: Comment améliorer ?

**R:** Trois pistes :
1. Prédire à court terme (1-2 ans au lieu de 5-10)
2. Ajouter les résultats électoraux précédents comme features
3. Accepter que certaines prédictions sont fondamentalement impossibles !

### Q: C'est un échec alors ?

**R:** NON ! C'est une **réussite scientifique** :
- On a prouvé que l'approche ne marche pas
- On a testé les deux sens
- On a identifié les limites
- On comprend POURQUOI ça ne marche pas
- C'est mieux qu'un modèle qui marche par chance sans comprendre !

---

## 8. Points forts à mettre en avant

1. ✅ **Rigueur scientifique** : Tests dans les deux sens
2. ✅ **Honnêteté** : On admet que ça ne marche pas
3. ✅ **Compréhension** : On explique pourquoi
4. ✅ **Maîtrise technique** : Anti-overfitting appliqué correctement
5. ✅ **Esprit critique** : Analyse des limites

**C'est un POC qui démontre les limites de la prédiction électorale avec des données socio-économiques. C'est une conclusion scientifiquement valable !** 🎯
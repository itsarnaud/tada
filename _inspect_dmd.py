import pandas as pd

df = pd.read_csv('data/raw/DATA 2014/DEMANDEUR EMPLOI/demandeur_emploi.csv', sep=';', encoding='utf-8-sig', nrows=5)
print('Columns:', df.columns.tolist())
print(df.to_string())

df_full = pd.read_csv('data/raw/DATA 2014/DEMANDEUR EMPLOI/demandeur_emploi.csv', sep=';', encoding='utf-8-sig', low_memory=False)
print('Shape:', df_full.shape)
print('Date unique:', sorted(df_full['Date'].unique()))
print('Sexe unique:', df_full['Sexe'].unique().tolist())
col_age = [c for c in df_full.columns if 'ge' in c.lower()][0]
print('Tranche age col:', col_age)
print('Tranche age unique:', df_full[col_age].unique().tolist())
print('Type donnees unique:', df_full['Type de données'].unique().tolist())
print('Categorie unique:', df_full['Catégorie'].unique().tolist())

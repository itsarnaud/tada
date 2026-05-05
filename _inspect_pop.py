import pandas as pd

fp = 'data/raw/DATA 2014/POPULATION TRANCHE/population_tranche_age.xls'
xl = pd.ExcelFile(fp)
print('Sheets:', xl.sheet_names)

# Lire les premières lignes de la première feuille
df = pd.read_excel(fp, sheet_name=0, header=None, nrows=10)
print(f'\nShape preview: {df.shape}')
for i, row in df.iterrows():
    print(f'row {i:2d}: {[str(v)[:30] for v in row.tolist()[:8]]}')

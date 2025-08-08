# Skript pro porovnání dvou XLS souborů na listu 'Detail'
# Vstupní soubory se dávají do adresáře 'vstup_compare'
# Výstupní soubor se ukládá do adresáře 'vystup_compare'
# Párování řádků podle sloupce 'PLATCE_ID', porovnávají se všechny sloupce kromě 'Report_ID', 'Report_Date', 'ZÁVAŽNOST' a všech obsahujících 'DALSICH'
# Výstup: Excel soubor se dvěma listy: 'Nové/Chybějící' a 'Rozdíly'
import pandas as pd
import sys
import os
from datetime import datetime

EXCLUDE_COLUMNS = ['REPORT_ID', 'REPORT_DATE', 'ZÁVAŽNOST']
KEY_COLUMN = 'PLATCE_ID'
INPUT_DIR = 'vstup_compare'
OUTPUT_DIR = 'vystup_compare'
now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'compareXLSDetail_report_{now_str}.xlsx')

if __name__ == "__main__":
    files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith('.xls') or f.lower().endswith('.xlsx')]
    if len(files) != 2:
        print(f"Chyba: Ve složce '{INPUT_DIR}' musí být přesně 2 Excel soubory pro porovnání. Aktuálně nalezeno: {len(files)}.")
        print("Zkontrolujte obsah složky a spusťte skript znovu.")
        sys.exit(1)
    file1, file2 = [os.path.join(INPUT_DIR, f) for f in files]
    print(f"Porovnávám soubory: {files[0]} a {files[1]}")
    df1 = pd.read_excel(file1, sheet_name='Detail')
    df2 = pd.read_excel(file2, sheet_name='Detail')
    # Odstranění sloupců, které se nemají porovnávat
    exclude_cols = [col for col in df1.columns if col.upper() in EXCLUDE_COLUMNS or 'DALSICH' in col.upper()]
    df1 = df1.drop(columns=exclude_cols, errors='ignore')
    exclude_cols2 = [col for col in df2.columns if col.upper() in EXCLUDE_COLUMNS or 'DALSICH' in col.upper()]
    df2 = df2.drop(columns=exclude_cols2, errors='ignore')
    df1 = df1.set_index(KEY_COLUMN)
    df2 = df2.set_index(KEY_COLUMN)
    # Nové/chybějící řádky
    only_in_1 = df1.loc[~df1.index.isin(df2.index)].reset_index()
    only_in_2 = df2.loc[~df2.index.isin(df1.index)].reset_index()
    only_in_1['Zdroj'] = files[0]
    only_in_2['Zdroj'] = files[1]
    new_missing = pd.concat([only_in_1, only_in_2], ignore_index=True)
    # Rozdíly v řádcích podle platce_id
    diffs = []
    common_ids = set(df1.index) & set(df2.index)
    for pid in common_ids:
        row1 = df1.loc[pid]
        row2 = df2.loc[pid]
        for col in df1.columns:
            if col.upper() in EXCLUDE_COLUMNS or 'DALSICH' in col.upper():
                continue
            val1 = row1[col] if col in row1 else None
            val2 = row2[col] if col in row2 else None
            if pd.isnull(val1) and pd.isnull(val2):
                continue
            if val1 != val2:
                # Získání hodnot CU_REF_NO a CA_REF_NO (stačí z prvního souboru, jsou vždy stejné)
                cu_ref_no = row1.get('CU_REF_NO', None)
                ca_ref_no = row1.get('CA_REF_NO', None)
                diff_row = {
                    KEY_COLUMN: pid,
                    'CU_REF_NO': cu_ref_no,
                    'CA_REF_NO': ca_ref_no,
                    'Sloupec': col,
                    files[0]: val1,
                    files[1]: val2
                }
                diffs.append(diff_row)
    diffs_df = pd.DataFrame(diffs)
    # Výstup do Excelu
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        new_missing.to_excel(writer, sheet_name='Nové_Chybějící', index=False)
        diffs_df.to_excel(writer, sheet_name='Rozdíly', index=False)
    print(f"Porovnání dokončeno. Výsledek v {OUTPUT_FILE}")

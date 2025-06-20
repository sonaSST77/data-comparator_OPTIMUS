import oracledb
import pandas as pd
import datetime
import os
from db_connect import get_db_connection

# Načtení čísla vlny (nebo více hodnot oddělených čárkou) ze souboru parametry/parametry.txt s klíčem "cislo_vlny" nebo zadání uživatelem
param_file = os.path.join("parametry", "parametry.txt")
cisla_vlny = None
if os.path.exists(param_file):
    with open(param_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip().startswith("cislo_vlny="):
                cisla_vlny = line.strip().split("=", 1)[1]
                break
if not cisla_vlny:
    cisla_vlny = input('Zadejte číslo vlny (nebo více hodnot oddělených čárkou): ')
else:
    print(f'Načteno číslo vlny ze souboru: {cisla_vlny}')
cisla_vlny_list = [v.strip() for v in cisla_vlny.split(',') if v.strip()]

try:
    connection = get_db_connection()
    print("Připojení k databázi bylo úspěšné!")

    cursor = connection.cursor()
    # Dotaz: pro každého zákazníka (ID_PLATCE) najdi nejstarší REPORT_DATE se stavem 'storno'
    query = """
    SELECT ID_PLATCE, CU_REF_NO , CA_REF_NO , MIN(REPORT_DATE) AS START_STORNO_DATE
    FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU
    WHERE STAV = 'storno'
    AND WAVE_ID IN ({})
    GROUP BY ID_PLATCE, CU_REF_NO , CA_REF_NO
    ORDER BY START_STORNO_DATE
    """.format(','.join([f':wave_id{i}' for i in range(len(cisla_vlny_list))]))
    params = {f'wave_id{i}': v for i, v in enumerate(cisla_vlny_list)}
    cursor.execute(query, params)
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    # Uložení do Excelu
    today = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "vystupy"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"statistika_storen_{today}.xlsx")
    df.to_excel(filename, index=False)
    print(f"Výsledek byl uložen do souboru {filename}")

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení nebo dotazu:", e)
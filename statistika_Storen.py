import oracledb
import pandas as pd
import datetime
import os

# Přihlašovací údaje
username = "so081267"
password = "msaDBSona666666"
dsn = "ocsxpptdb02r-scan.ux.to2cz.cz:1521/COMSA07R"

# Načtení čísla vlny ze souboru parametey.txt
param_file = "parametry.txt"
if os.path.exists(param_file):
    with open(param_file, "r", encoding="utf-8") as f:
        cislo_vlny = f.read().strip()
    if not cislo_vlny:
        cislo_vlny = "202506010001"
        print('Soubor byl prázdný, použita defaultní vlna "202506010001"')
    else:
        print(f'Načteno číslo vlny ze souboru: {cislo_vlny}')
else:
    cislo_vlny = "202506010001"
    print('Soubor parametey.txt nenalezen, použita defaultní vlna "202506010001"')

try:
    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )
    print("Připojení k databázi bylo úspěšné!")

    cursor = connection.cursor()
    # Dotaz: pro každého zákazníka (ID_PLATCE) najdi nejstarší REPORT_DATE se stavem 'storno'
    query = """
    SELECT ID_PLATCE, CU_REF_NO , CA_REF_NO , MIN(REPORT_DATE) AS START_STORNO_DATE
    FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU
    WHERE STAV = 'storno'
    AND WAVE_ID = :wave_id
    GROUP BY ID_PLATCE, CU_REF_NO , CA_REF_NO
    ORDER BY START_STORNO_DATE
    """
    cursor.execute(query, {"wave_id": cislo_vlny})
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
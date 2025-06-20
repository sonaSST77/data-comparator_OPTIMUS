import oracledb
import pandas as pd
import datetime
import os
from db_connect import get_db_connection

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


now = datetime.datetime.now()
dnes = now.strftime("%d-%m-%Y")
vcera = (now - datetime.timedelta(days=1)).strftime("%d-%m-%Y")

try:
    connection = get_db_connection()
    print("Připojení k databázi bylo úspěšné!")
    cursor = connection.cursor()
    print(f"Dnešní datum: {dnes}")

    # Nejprve zjistíme, jestli existují záznamy s dnešním datem
    kontrola_query = """
    SELECT COUNT(*) FROM MIGUSERP.REP_REKONCIL_O2_SLUZBY
    WHERE REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
    """
    cursor.execute(kontrola_query, {"report_date": dnes})
    pocet = cursor.fetchone()[0]

    if pocet > 0:
        datum = dnes
        print(f"Používá se dnešní datum: {datum}")
    else:
        datum = vcera
        print(f"Nebyly nalezeny záznamy s dnešním datem, používá se včerejší datum: {datum}")

    cursor = connection.cursor()
    # Vybírám pouze nestornované plátce v AISA a kontroluju jestli nemají Deactivovanou služdu
    # TV můžeme v O2 rušit hned a Inet k poslednímu v měsíci
    query = """
    SELECT * FROM						
    MIGUSERP.REP_REKONCIL_O2_SLUZBY rros											
    WHERE REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
    AND WAVE_ID = :wave_id
    AND PLATCE_ID NOT IN (							
        SELECT DISTINCT ID_PLATCE							
        FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU rrsve							
            WHERE RRSVE.WAVE_ID = :wave_id
            AND REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
            AND STAV = 'storno'							
    )										
    """
    cursor.execute(query, {"wave_id": cislo_vlny, "report_date": datum})
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    # Uložení do Excelu
    today = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "vystupy"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"statistika_sluzeb_{today}.xlsx")

    # Uložení hlavního DataFrame na první list a příprava druhého listu
    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Detail")

        # Druhý list: výběr požadovaných sloupců a nejstarší REPORT_DATE pro konkrétní WAVE_ID
        query2 = """
        SELECT PLATCE_ID, CU_REF_NO, CA_REF_NO, MIN(REPORT_DATE) AS REPORT_DATE
        FROM MIGUSERP.REP_REKONCIL_O2_SLUZBY
        WHERE WAVE_ID = :wave_id
        AND ZÁVAŽNOST = 'Error'
        GROUP BY PLATCE_ID, CU_REF_NO, CA_REF_NO
        """
        cursor2 = connection.cursor()
        cursor2.execute(query2, {"wave_id": cislo_vlny})
        columns2 = [col[0] for col in cursor2.description]
        data2 = cursor2.fetchall()
        df2 = pd.DataFrame(data2, columns=columns2)
        df2.to_excel(writer, index=False, sheet_name="Souhrn")
        cursor2.close()

    print(f"Výsledek byl uložen do souboru {filename}")

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení nebo dotazu:", e)

# Všechny další odpovědi budou v češtině.
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


now = datetime.datetime.now()
dnes = now.strftime("%d-%m-%Y")
vcera = (now - datetime.timedelta(days=1)).strftime("%d-%m-%Y")

try:
    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )
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
    df.to_excel(filename, index=False)
    print(f"Výsledek byl uložen do souboru {filename}")

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení nebo dotazu:", e)
import oracledb
import pandas as pd
import datetime
import os

# Přihlašovací údaje
username = "so081267"
password = "msaDBSona666666"
dsn = "ocsxpptdb02r-scan.ux.to2cz.cz:1521/COMSA07R"

# Zadejte číslo vlny jako vstupní parametr
cislo_vlny = input("Zadejte číslo vlny (default pouzita 202506010001): ")
if not cislo_vlny:
    cislo_vlny = "202506010001"
    print('Byla použita defaultní vlna "202506010001"')

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
    SELECT COUNT(*) FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU
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
    # Dotaz: pro každého zákazníka (ID_PLATCE) najdi nejstarší REPORT_DATE se stavem 'storno'
    query = """
    SELECT * FROM						
    MIGUSERP.REP_REKONCIL_O2_SLUZBY rros											
    WHERE REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
    AND WAVE_ID = :wave_id				
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
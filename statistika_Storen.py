import oracledb
import pandas as pd
import datetime

# Přihlašovací údaje
username = "so081267"
password = "msaDBSona666666"
dsn = "ocsxpptdb02r-scan.ux.to2cz.cz:1521/COMSA07R"

# Zadejte číslo vlny jako vstupní parametr
cislo_vlny = input("Zadejte číslo vlny (default pouzita 202506010001): ")
if not cislo_vlny:
    cislo_vlny = "202506010001"
    print('Byla použita defaultní vlna "202506010001"')

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
    SELECT ID_PLATCE, MIN(REPORT_DATE) AS START_STORNO_DATE
    FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU
    WHERE STAV = 'storno'
    AND WAVE_ID = :wave_id
    GROUP BY ID_PLATCE
    ORDER BY START_STORNO_DATE
    """
    cursor.execute(query, {"wave_id": cislo_vlny})
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    # Uložení do Excelu
    today = datetime.datetime.now().strftime("%Y%m%d")
    filename = f"statistika_storen_{today}.xlsx"
    df.to_excel(filename, index=False)
    print(f"Výsledek byl uložen do souboru {filename}")

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení nebo dotazu:", e)
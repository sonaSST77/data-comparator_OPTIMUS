import oracledb
print("oracledb je nainstalováno správně")

# Zadejte své údaje
username = "so081267"
password = "msaDBSona666666"
dsn = "ocsxpptdb02r-scan.ux.to2cz.cz:1521/COMSA07R"  # např. "localhost:1521/COMSAR"

try:
    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )
    print("Připojení k databázi bylo úspěšné!")

# Vytvoření kurzoru a provedení dotazu
    cursor = connection.cursor()
    cursor.execute(
    "SELECT * FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU rrsve WHERE RRSVE.WAVE_ID IN ('202506010001') AND rrsve.STAV = 'storno' AND REPORT_DATE > TO_DATE('01-06-2025','DD-MM-YYYY')"
    )  # Změňte 'tabulka' na název vaší tabulky

    # Výpis výsledků
    for row in cursor:
        print(row)

    cursor.close()


    connection.close()
except Exception as e:
    print("Chyba při připojení:", e)
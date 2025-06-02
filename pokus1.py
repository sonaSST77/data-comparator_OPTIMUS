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
    cursor.execute("SELECT ID, DESCRIPTION  FROM MIGUSERP.LX_CETIN_PM_MIGRATION_WAVE lcpmw WHERE ID LIKE '2025%'")  # Změňte 'tabulka' na název vaší tabulky

    # Výpis výsledků
    for row in cursor:
        print(row)

    cursor.close()


    connection.close()
except Exception as e:
    print("Chyba při připojení:", e)
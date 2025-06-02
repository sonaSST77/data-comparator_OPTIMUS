import oracledb
import pandas as pd
print("oracledb je nainstalováno správně")

# Zadejte své údaje
username = "so081267"
password = "msaDBSona666666"
dsn = "ocsxpptdb02r-scan.ux.to2cz.cz:1521/COMSA07R"  # např. "localhost:1521/COMSAR"

# Zadejte číslo vlny jako vstupní parametr
cislo_vlny = input("Zadejte číslo vlny: ")  # zde můžete použít input() pro zadání od uživatele
if not cislo_vlny:
    cislo_vlny = "202506010001" #cervnova vlna

try:
    connection = oracledb.connect(
        user=username,
        password=password,
        dsn=dsn
    )
    print("Připojení k databázi bylo úspěšné!")

# Vytvoření kurzoru a provedení dotazu
    cursor = connection.cursor()
    query = """
    SELECT * FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU rrsve
    WHERE RRSVE.WAVE_ID = :wave_id
      AND rrsve.STAV = 'storno'
      AND REPORT_DATE > TO_DATE('01-06-2025','DD-MM-YYYY')
    """
    cursor.execute(query, {"wave_id": cislo_vlny})

# Načtení výsledků do pandas DataFrame
    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

# Připravit DataFrame s textem dotazu
    df_query = pd.DataFrame({"SQL dotaz": [query]})

    # Uložení do Excelu na dva listy
    with pd.ExcelWriter("vysledek.xlsx") as writer:
        df.to_excel(writer, sheet_name="Výsledek", index=False)
        df_query.to_excel(writer, sheet_name="SQL_dotaz", index=False)

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení:", e)
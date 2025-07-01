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
# Zpracování na seznam hodnot
cisla_vlny_list = [v.strip() for v in cisla_vlny.split(',') if v.strip()]


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
    # úprava SQL dotazu pro více hodnot
    query = """
    SELECT * FROM
    MIGUSERP.REP_REKONCIL_O2_SLUZBY rros
    WHERE REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
    AND WAVE_ID IN ({})
    AND PLATCE_ID NOT IN (
        SELECT DISTINCT ID_PLATCE
        FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU rrsve
            WHERE RRSVE.WAVE_ID IN ({})
            AND REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
            AND STAV = 'storno'
    )
    """.format(','.join([f':wave_id{i}' for i in range(len(cisla_vlny_list))]), ','.join([f':wave_id{i}' for i in range(len(cisla_vlny_list))]))
    # Příprava parametrů
    params = {f'wave_id{i}': v for i, v in enumerate(cisla_vlny_list)}
    params['report_date'] = datum
    cursor.execute(query, params)
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

        # Druhý list: nový výběr dat bez podmínky status_reason NOT LIKE ('STOP%')
        query2 = """
        SELECT * FROM
            MIGUSERP.REP_REKONCIL_O2_SLUZBY rros
            WHERE REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
            AND WAVE_ID IN ({})
        """.format(','.join([f':wave_id{i}' for i in range(len(cisla_vlny_list))]))
        params2 = {f'wave_id{i}': v for i, v in enumerate(cisla_vlny_list)}
        params2['report_date'] = datum
        cursor2 = connection.cursor()
        cursor2.execute(query2, params2)
        columns2 = [col[0] for col in cursor2.description]
        data2 = cursor2.fetchall()
        df2 = pd.DataFrame(data2, columns=columns2)
        df2.to_excel(writer, index=False, sheet_name="Souhrn")
        cursor2.close()

        # Třetí list: SQL dotazy pro přehled
        sql_overview = pd.DataFrame({
            'Dotaz': ['Detail', 'Souhrn'],
            'SQL': [
                query.strip().replace('\n', ' ').replace('    ', ' '),
                query2.strip().replace('\n', ' ').replace('    ', ' ')
            ]
        })
        sql_overview.to_excel(writer, index=False, sheet_name="SQL_dotazy")

    print(f"Výsledek byl uložen do souboru {filename}")

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení nebo dotazu:", e)

# Všechny další odpovědi budou v češtině.
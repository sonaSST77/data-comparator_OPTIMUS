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
    # úprava SQL dotazu pro více hodnot
    query = """
    SELECT * FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU rrsve
    WHERE RRSVE.WAVE_ID IN ({})
      AND rrsve.STAV = 'storno'
      AND REPORT_DATE >= TO_DATE(:report_date, 'DD-MM-YYYY')
    """.format(','.join([f':wave_id{i}' for i in range(len(cisla_vlny_list))]))
    params = {f'wave_id{i}': v for i, v in enumerate(cisla_vlny_list)}
    params['report_date'] = datum
    cursor.execute(query, params)

    columns = [col[0] for col in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)

    # Získání všech ID_PLATCE z výsledku
    id_platce_list = df["ID_PLATCE"].unique().tolist()

    # Výsledky pro všechny ID_PLATCE
    vysledky = []

    for id_platce in id_platce_list:
        # Zde napište svůj druhý dotaz, např.:
        query2 = """
        SELECT *
        FROM MIGUSERP.REP_REKONCIL_O2_SLUZBY 
        WHERE PLATCE_ID = :id_platce 
          AND WAVE_ID IN ({})
          AND REPORT_DATE > TO_DATE(:report_date, 'DD-MM-YYYY')
        """.format(','.join([f':wave_id{i}' for i in range(len(cisla_vlny_list))]))
        params2 = {f'wave_id{i}': v for i, v in enumerate(cisla_vlny_list)}
        params2['id_platce'] = id_platce
        params2['report_date'] = datum
        cursor.execute(query2, params2)
        data2 = cursor.fetchall()
        columns2 = [col[0] for col in cursor.description]
        for row in data2:
            vysledky.append(dict(zip(columns2, row)))

    # Pokud chcete uložit výsledky do Excelu na další list:
    if vysledky:
        df2 = pd.DataFrame(vysledky)
    else:
        df2 = pd.DataFrame()

    # Výběr záznamů, kde STAV_TV_SLUZEB není prázdné (není None ani NaN ani "")
    if not df2.empty and "STAV_TV_SLUZEB" in df2.columns:
        df3 = df2[df2["STAV_TV_SLUZEB"].notnull() & (df2["STAV_TV_SLUZEB"] != "")].copy()
    else:
        df3 = pd.DataFrame()

    # Vytvoření názvu souboru s datem a časem, uložení do podsložky 'vystupy'
    today = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = "vystupy"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, f"Reconcil_pred_TO_{today}.xlsx")

    if not df3.empty and "PLATCE_ID" in df3.columns:
        platci = df3["PLATCE_ID"].unique().tolist()
        format_strings = ','.join([':id'+str(i) for i in range(len(platci))])
        query_min = f"""
            SELECT ID_PLATCE, MIN(REPORT_DATE) AS MIN_REPORT_DATE
            FROM MIGUSERP.REP_REKONCIL_STAV_V_EDENIKU
            WHERE ID_PLATCE IN ({format_strings})
            GROUP BY ID_PLATCE
        """
        params = {f'id{i}': platci[i] for i in range(len(platci))}
        cursor.execute(query_min, params)
        min_dates = cursor.fetchall()
        min_dates_dict = {row[0]: row[1] for row in min_dates}
        df3["MIN_REPORT_DATE"] = df3["PLATCE_ID"].map(min_dates_dict)
        # Přesunout MIN_REPORT_DATE na první pozici
        cols = df3.columns.tolist()
        cols.insert(0, cols.pop(cols.index("MIN_REPORT_DATE")))
        df3 = df3[cols]

     # Uložení do Excelu na tri listy
    with pd.ExcelWriter(filename) as writer:
        df.to_excel(writer, sheet_name="Storno_denik", index=False)
        df2.to_excel(writer, sheet_name="O2_sluzby_stornovanych", index=False)
        df3.to_excel(writer, sheet_name="TV_sluzby_aktivni", index=False)

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení:", e)
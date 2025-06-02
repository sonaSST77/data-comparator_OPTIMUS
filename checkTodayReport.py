import oracledb
import pandas as pd
import datetime
import requests


# Zadejte své údaje
username = "so081267"
password = "msaDBSona666666"
dsn = "ocsxpptdb02r-scan.ux.to2cz.cz:1521/COMSA07R"  # např. "localhost:1521/COMSAR"

# Teams webhook URL (vložte svůj vlastní)
webhook_url = "https://outlook.office.com/webhook/TVUJ_WEBHOOK_URL"

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
        print(f"Máme dnešní data v reportu, používá se dnešní datum!!!")
        # Odeslání zprávy do Teams
        zprava = {
            "text": f"V databázi jsou data pro dnešní den {dnes}."
        }
        response = requests.post(webhook_url, json=zprava)
        if response.status_code == 200:
            print("Zpráva byla odeslána do Teams.")
        else:
            print("Chyba při odesílání do Teams:", response.text)
    else:
        datum = vcera
        print(f"Nemáme dnešní data v reportu, budeme čekat!!!")

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení nebo dotazu:", e)
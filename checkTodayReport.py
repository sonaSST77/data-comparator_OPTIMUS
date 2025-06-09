import oracledb
import pandas as pd
import datetime
import smtplib
from email.mime.text import MIMEText
import os


# Zadejte své údaje
username = "so081267"
password = "msaDBSona666666"
dsn = "ocsxpptdb02r-scan.ux.to2cz.cz:1521/COMSA07R"  # např. "localhost:1521/COMSAR"

# Nastavení e-mailu
odesilatel = "reconcilOptimus@o2.cz"
#prijemce = "461f4702.telcocloud.onmicrosoft.com@emea.teams.ms"  # Teams kanál má speciální e-mail, získejte jej v Teams
prijemce = "sona.stradova@o2.cz"  # Teams kanál má speciální e-mail, získejte jej v Teams
smtp_server = "smtp.cz.o2"   # nebo např. smtp.cz.o2
smtp_port = 25

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
        # Odeslání e-mailu
        subject = "Report: Data pro dnešní den jsou k dispozici"
        body = f"V databázi jsou data pro datum {dnes}."
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = odesilatel
        msg["To"] = prijemce

        with smtplib.SMTP(smtp_server, smtp_port) as server:
          server.sendmail(odesilatel, prijemce, msg.as_string())
        print("E-mail byl odeslán.")

         # Vytvoření textového souboru s aktuálním datem a časem v adresáři 'vystupy'
        output_dir = "vystupy"
        os.makedirs(output_dir, exist_ok=True)
        now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"report_ready_{now_str}.txt"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"Data pro datum {dnes} byla nalezena v {now_str}.\n")
        print(f"Soubor {filepath} byl vytvořen.")

    else:
        datum = vcera
        print(f"Nemáme dnešní data v reportu, budeme čekat!!!")

    cursor.close()
    connection.close()
except Exception as e:
    print("Chyba při připojení nebo dotazu:", e)
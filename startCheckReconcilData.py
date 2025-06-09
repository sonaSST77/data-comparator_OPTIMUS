import schedule
import time
import os
from datetime import datetime, timedelta


# Kontrola, jestli už nemáme prověřeno
today_str = datetime.now().strftime("%Y%m%d")
# Kontrola, zda existuje soubor signalizující odeslání e-mailu s dnešním datem v názvu
if any(f.startswith("report_ready_") and today_str in f and f.endswith(".txt") for f in os.listdir("vystupy")):
    print("Data v reportech pro dnešek už máme, e-mail byl odeslán.")
    exit()


print("Spouštím kontrolu")

def spustit_kontrolu():
    print("Spouštím program pro kontrolu dat", datetime.now())
    os.system("python checkTodayReport.py")

# Spustit kontrolu hned na začátku
spustit_kontrolu()

# Spouštět každých 10 minut
schedule.every(10).minutes.do(spustit_kontrolu)

start_time = datetime.now()
end_time = start_time + timedelta(hours=1)

while True:

    # Získání dnešního data ve formátu YYYYMMDD
    today_str = datetime.now().strftime("%Y%m%d")
    # Kontrola, zda existuje soubor signalizující odeslání e-mailu s dnešním datem v názvu
    if any(f.startswith("report_ready_") and today_str in f and f.endswith(".txt") for f in os.listdir("vystupy")):
        print("Soubor existuje, e-mail byl odeslán, ukončuji skript.")
        break

    now = datetime.now()
    print(f"[{now.strftime('%H:%M:%S')}] Skript stále běží...")
    if now >= end_time:
        print("Uplynula 1 hodina, ukončuji skript.")
        break
    schedule.run_pending()
    time.sleep(30)
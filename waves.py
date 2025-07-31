# Skript pro export dat z tabulky MIGUSERP.LX_CETIN_PM_MIGRATION_WAVE
# Výsledek je uložen do souboru vystupy/vlny/waves.txt
import oracledb
import os
from db_connect import get_db_connection

if __name__ == "__main__":
    os.makedirs("vystupy/vlny", exist_ok=True)
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ID, DESCRIPTION FROM MIGUSERP.LX_CETIN_PM_MIGRATION_WAVE ORDER BY ID")
    rows = cursor.fetchall()
    with open("vystupy/vlny/waves.txt", "w", encoding="utf-8") as f:
        for row in rows:
            f.write(f"{row}\n")
    cursor.close()
    conn.close()

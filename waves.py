import oracledb
from db_connect import get_db_connection

if __name__ == "__main__":
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT ID, DESCRIPTION FROM MIGUSERP.LX_CETIN_PM_MIGRATION_WAVE")
    rows = cursor.fetchall()
    with open("waves.txt", "w", encoding="utf-8") as f:
        for row in rows:
            f.write(f"{row}\n")
    cursor.close()
    conn.close()

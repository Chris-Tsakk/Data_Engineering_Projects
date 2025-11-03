import pandas as pd
from sqlalchemy import create_engine, text
import csv

# 1) Σύνδεση
PORT = 5433
engine = create_engine(f"postgresql+psycopg2://etluser:EtlPass123@127.0.0.1:{PORT}/etldb")

# 2) Διαβάζουμε το CSV και κάνουμε ΚΑΘΑΡΙΣΜΑ HEADERS ώστε να ταιριάζουν με το df:
#    -> month, 1958, 1959, 1960
df_raw = pd.read_csv("data.csv")

# καθάρισμα ονομάτων στηλών: βγάλε κενά/εισαγωγικά και ομογενοποίησε όπως στο df
df_raw.columns = [c.strip().strip('"') for c in df_raw.columns]
df_raw.columns = [col.strip().lower().replace(" ", "_") for col in df_raw.columns]

# 3) LOAD RAW στον Postgres με αυτά ΑΚΡΙΒΩΣ τα ονόματα
df_raw.to_sql("air_travel_raw", engine, if_exists="replace", index=False)

# (προαιρετικό) δείξε τι έγραψε ο Postgres ως ονόματα στηλών
print(pd.read_sql("""
SELECT column_name
FROM information_schema.columns
WHERE table_schema='public' AND table_name='air_travel_raw'
ORDER BY ordinal_position
""", engine))

# 4) ELT μέσα στη DB (SQL): χρησιμοποιούμε "1958","1959","1960" με quotes
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS air_travel_elt;"))
    conn.execute(text("""
        CREATE TABLE air_travel_elt AS
        SELECT
            month,                 -- εδώ είναι απλό όνομα (πεζά), δεν χρειάζεται quotes
            "1958"::int AS passengers_1958,
            "1959"::int AS passengers_1959,
            "1960"::int AS passengers_1960
        FROM air_travel_raw
        WHERE "1958"::int > 300;
    """))
    conn.commit()

# 5) Verification
print(pd.read_sql("SELECT * FROM air_travel_elt ORDER BY month LIMIT 5;", engine))

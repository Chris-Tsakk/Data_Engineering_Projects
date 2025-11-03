from sqlalchemy import create_engine, text
import pandas as pd
import csvhttps://github.com/Chris-Tsakk/Data_Engineering_Projects
df = pd.read_csv("data.csv")
print(list(df.columns))
print("-----------------------------")
print(df.head())# προεπισκόπηση
print("-----------------------------")
print(df.dtypes)      # τύποι δεδομένων
print("-----------------------------")
df.columns = [c.strip().strip('"') for c in df.columns]
df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
df['1958'] = df['1958'].astype(int) # Example transformation
df_filtered = df[df['1958'] > 300]
# ----- CHANGE PORT HERE IF YOU USED 5433 -----
PORT = 5433
engine = create_engine(f"postgresql+psycopg2://etluser:EtlPass123@127.0.0.1:{PORT}/etldb")

with engine.connect() as conn:
    print(conn.execute(text("SELECT current_user, current_database();")).fetchone())
# Load & verify
df_filtered.to_sql("air_travel_etl", engine, if_exists="replace", index=False)
out = pd.read_sql("SELECT * FROM air_travel_etl ORDER BY month LIMIT 5;", engine)
print(out)
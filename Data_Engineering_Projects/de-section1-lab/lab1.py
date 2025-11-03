from pathlib import Path
import pandas as pd
import numpy as np
import duckdb
import pandera.pandas as pdr   # 🟢 άλλαξέ το από "pa" σε "pdr"
from pandera import Column, Check

# Ορισμός των paths για τα 3 επίπεδα δεδομένων
BRONZE = Path("data/bronze")
SILVER = Path("data/silver")
GOLD = Path("data/gold")

# Δημιουργία φακέλων αν δεν υπάρχουν
for p in [BRONZE, SILVER, GOLD]:
    p.mkdir(parents=True, exist_ok=True)

# Διεύθυνση του dataset (προέρχεται από το seaborn)
RAW_URL = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"

# Συνάρτηση για λήψη των δεδομένων
def extract_to_bronze():
    # Διαβάζουμε το dataset από το URL
    df = pd.read_csv(RAW_URL)

    # Αποθηκεύουμε το dataset σε τοπικό CSV στον φάκελο bronze
    out_path = BRONZE / "tips_raw.csv"
    df.to_csv(out_path, index=False)

    print(f"[BRONZE] Wrote {out_path} with {len(df)} rows")
    return out_path

def transform_to_silver(bronze_path: Path):
    df= pd.read_csv(bronze_path)

    df.columns=[c.strip().lower().replace(" ", "_") for c in df.columns]
    df["tip_pct"] = (df["tip"] / df["total_bill"]).round(4)
    df["visit_datetime"] = pd.date_range("2024-01-01", periods=len(df), freq="h")

    cat_cols = ["sex", "smoker", "day", "time"]
    for c in cat_cols:
        df[c] = df[c].astype("category")
        out_path = SILVER / "tips_clean.parquet"
        df.to_parquet(out_path, index=False)

        print(f"[SILVER] Wrote {out_path} with {len(df)} rows")
        return out_path

# -----------------------------------------------
# ΜΕΡΟΣ D – Gold
# Δημιουργία συγκεντρωτικού πίνακα από τα Silver δεδομένα
# -----------------------------------------------

def aggregate_to_gold(silver_path: Path):
    # Διαβάζουμε το αρχείο Parquet από το Silver επίπεδο
    df = pd.read_parquet(silver_path)   # 🟥 pd.read_parquet() → φορτώνει τα καθαρισμένα δεδομένα σε DataFrame

    # Δημιουργούμε έναν νέο πίνακα "summary" με ομαδοποίηση
    summary = (
        df.groupby(["sex", "smoker", "day"], as_index=False)  # 🟥 groupby() → ομαδοποιεί τα δεδομένα ανά φύλο, κάπνισμα, ημέρα
          .agg(
              avg_tip_pct=("tip_pct", "mean"),                # 🟥 υπολογίζει τον μέσο όρο του tip_pct
              total_revenue=("total_bill", "sum"),            # 🟥 υπολογίζει το άθροισμα του total_bill
              rows=("tip_pct", "count")                       # 🟥 μετράει πόσες γραμμές έχει κάθε ομάδα
          )
    )

    # Ορίζουμε το path εξόδου για το Gold επίπεδο
    out_path = GOLD / "tips_summary.parquet"  # 🟥 Path join operator "/"

    # Αποθηκεύουμε το αποτέλεσμα σε Parquet
    summary.to_parquet(out_path, index=False)  # 🟥 to_parquet() → αποθηκεύει τα συγκεντρωτικά δεδομένα
    print(f"[GOLD] Wrote {out_path} with {len(summary)} rows")  # 🟥 print() → εμφάνιση πληροφοριών

    return out_path  # 🟥 επιστρέφει το path του αρχείου για χρήση σε επόμενο στάδιο

def query_examples():
    # Δημιουργούμε μια προσωρινή σύνδεση (in-memory database)
    con = duckdb.connect(database=":memory:")  # 🟥 ":memory:" σημαίνει ότι δεν δημιουργεί αρχείο, απλώς κρατά τα δεδομένα στη RAM

    # --------------------------
    # 1️⃣ Ερώτημα πάνω στο Silver επίπεδο
    # --------------------------
    q1 = con.execute("""
        SELECT 
            sex, 
            smoker, 
            AVG(tip_pct) AS avg_tip_pct,       -- μέσο ποσοστό φιλοδωρήματος
            SUM(total_bill) AS revenue         -- συνολικά έσοδα
        FROM read_parquet('data/silver/tips_clean.parquet')  -- 🟥 DuckDB διαβάζει απευθείας Parquet αρχείο
        GROUP BY 1, 2                          -- 🟥 ομαδοποιεί ανά sex και smoker
        ORDER BY avg_tip_pct DESC              -- 🟥 ταξινομεί κατά μέσο ποσοστό
    """).df()                                  # 🟥 .df() → μετατρέπει το αποτέλεσμα σε pandas DataFrame

    print("\n[SQL on Silver]\n", q1.head())    # 🟥 Εμφανίζει τις πρώτες γραμμές του αποτελέσματος

    # --------------------------
    # 2️⃣ Ερώτημα πάνω στο Gold επίπεδο
    # --------------------------
    q2 = con.execute("""
        SELECT 
            day, 
            AVG(avg_tip_pct) AS avg_tip_pct    -- μέσος όρος των μέσων ποσοστών ανά ημέρα
        FROM read_parquet('data/gold/tips_summary.parquet')
        GROUP BY day
        ORDER BY avg_tip_pct DESC
    """).df()

    print("\n[SQL on Gold]\n", q2)             # 🟥 Εμφανίζει το αποτέλεσμα ομαδοποίησης ανά ημέρα
def validate_silver(silver_path: Path):
    # Ορισμός "σχήματος" ποιότητας δεδομένων (DataFrameSchema)
    schema = pdr.DataFrameSchema({
        "total_bill": Column(float, Check.ge(0)),        # 🟥 πρέπει να είναι >= 0
        "tip": Column(float, Check.ge(0)),               # 🟥 tip >= 0
        "tip_pct": Column(float, Check.in_range(0, 1)),  # 🟥 0 <= tip_pct <= 1
        "size": Column(int, Check.ge(1)),                # 🟥 μέγεθος παρέας >= 1
        "visit_datetime": Column(pdr.DateTime),           # 🟥 πρέπει να είναι έγκυρη ημερομηνία
    }, coerce=True)  # 🟥 coerce=True → μετατρέπει αυτόματα τύπους αν χρειάζεται

    # Διαβάζουμε το Silver αρχείο
    df = pd.read_parquet(silver_path)     # 🟥 φορτώνει το Silver dataset σε DataFrame

    # Εφαρμόζουμε το schema (έλεγχο)
    schema.validate(df, lazy=True)        # 🟥 ελέγχει όλους τους κανόνες και εμφανίζει αν υπάρχει σφάλμα

    print("[QUALITY] Silver passed validation!")  # 🟢 Αν δεν υπάρχουν λάθη → OK
# Εκτελείται αν τρέξουμε το αρχείο κατευθείαν
if __name__ == "__main__":
    bronze_file = extract_to_bronze()              # 🟤 Bronze → κατέβασε CSV
    silver_file = transform_to_silver(bronze_file) # ⚪ Silver → καθάρισε δεδομένα
    validate_silver(silver_file)                   # ✅ Έλεγχος ποιότητας δεδομένων
    gold_file = aggregate_to_gold(silver_file)     # 🟡 Gold → συγκεντρωτικά
    query_examples()                               # 🦆 SQL Queries → αναφορές
import hashlib
import json
import os
import sqlite3
import shutil
import pandas as pd
from pathlib import Path
import uuid

BASE_DIR = Path(__file__).resolve().parent.parent

DB_PATH = BASE_DIR / "data" / "expenses.db"
INGRESS_DIR = BASE_DIR / ".file_watch" / "ingress"
PROCESSED_DIR = BASE_DIR / ".file_watch" / "processed"

# TODO: Revisit this import-time directory creation; move it into an explicit setup step so the module stays side-effect-light and easier to test.
# TODO: Tighten file discovery so it ignores non-files and uses a stricter allowlist for incoming CSVs.
# TODO: Reject blank or missing descriptions as invalid rows before inserting them.
# TODO: Make duplicate handling more explicit in logs and metrics so skipped rows are distinguishable from newly inserted ones.
INGRESS_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def ensure_db_accessible():
    """Fails fast if the database path cannot be used for storage."""
    if DB_PATH.exists() and not os.access(DB_PATH, os.W_OK):
        raise PermissionError(f"Database file is not writable: {DB_PATH}")

    if not DB_PATH.parent.exists():
        raise FileNotFoundError(f"Database directory does not exist: {DB_PATH.parent}")

    if not os.access(DB_PATH.parent, os.W_OK):
        raise PermissionError(f"Database directory is not writable: {DB_PATH.parent}")


def init_db():
    """Initializes the database schema with separate tables for transactions and dynamic rules."""
    ensure_db_accessible()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 1. Transactions Table (with unique ID constraint to prevent duplicates)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            dedup_key TEXT UNIQUE,
            date TEXT,
            description TEXT,
            amount REAL,
            type TEXT,
            category TEXT
        )
    ''')
    
    # 2. Dynamic Rules Table (stores learned merchant-to-category maps)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS category_rules (
            keyword TEXT PRIMARY KEY,
            category TEXT
        )
    ''')
    
    # Seed a few basic Indian merchant rules if the table is completely brand new
    cursor.execute("SELECT COUNT(*) FROM category_rules")
    if cursor.fetchone()[0] == 0:
        initial_rules = [
            ('zepto', 'Groceries'),
            ('blinkit', 'Groceries'),
            ('swiggy', 'Dining Out'),
            ('zomato', 'Dining Out'),
            ('uber', 'Transport'),
            ('ola', 'Transport'),
            ('namma', 'Transport'),  # For Namma Yatri
            ('jio', 'Utilities/Bills'),
            ('airtel', 'Utilities/Bills')
        ]
        cursor.executemany("INSERT INTO category_rules (keyword, category) VALUES (?, ?)", initial_rules)
        conn.commit()

    conn.close()

def load_rules_from_db():
    """Fetches all active rules from the database as a dictionary."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT keyword, category FROM category_rules")
    rules = {row[0].lower(): row[1] for row in cursor.fetchall()}
    conn.close()
    return rules

def get_category(description, rules_dict):
    """Matches Indian merchant descriptors against active database rules."""
    if not isinstance(description, str):
        return "Uncategorized"
    
    desc_lower = description.lower()
    for keyword, category in rules_dict.items():
        if keyword in desc_lower:
            return category
    return "Uncategorized"

def generate_row_id():
    """Creates a surrogate row identifier for each inserted transaction."""
    return uuid.uuid4().hex


def generate_dedup_key(row):
    """Builds a stable fingerprint for logical-duplicate detection."""
    payload = {
        "date": str(row["Date"]).strip(),
        "description": str(row["Description"]).strip().lower(),
        "amount": float(row["Amount"]),
        "type": str(row["Type"]).strip(),
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def process_new_statements():
    """Main execution loop for cleaning and injecting raw CSV files."""
    init_db()
    
    # Target all CSVs waiting in the ingress folder
    files = [
        entry.name
        for entry in INGRESS_DIR.iterdir()
        if entry.is_file() and is_safe_file_name(entry.name)
    ]
    if not files:
        print("No new financial statements found in ingress/ folder.")
        return

    rules_dict = load_rules_from_db()
    conn = sqlite3.connect(DB_PATH)
    
    for file_name in files:
        file_path = INGRESS_DIR / file_name
        print(f"Parsing statement: {file_name}")
        
        try:
            if not is_safe_file_name(file_name):
                print(f"Skipping {file_name}: Unsafe file name.")
                continue

            df = pd.read_csv(file_path)
            df.columns = df.columns.str.strip()  # Clear accidental spaces in headers
            df = validate_dataframe(df, file_name)

            # Standardize column structures
            df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
            df['Amount'] = df['Amount'].astype(float)
            df['Type'] = df['Amount'].apply(lambda x: 'Income' if x > 0 else 'Expense')
            df['Category'] = df['Description'].apply(lambda d: get_category(d, rules_dict))
            df['id'] = [generate_row_id() for _ in range(len(df))]
            df['dedup_key'] = df.apply(generate_dedup_key, axis=1)

            records = df[['id', 'dedup_key', 'Date', 'Description', 'Amount', 'Type', 'Category']].to_dict(orient='records')
            inserted_count = 0
            conn.execute('BEGIN')
            try:
                for record in records:
                    try:
                        conn.execute('''
                            INSERT INTO transactions (id, dedup_key, date, description, amount, type, category)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            record['id'],
                            record['dedup_key'],
                            record['Date'],
                            record['Description'],
                            record['Amount'],
                            record['Type'],
                            record['Category'],
                        ))
                        inserted_count += 1
                    except sqlite3.IntegrityError:
                        pass  # Gracefully skip if this logical transaction was already imported

                conn.commit()
                print(f"Successfully tracked {inserted_count} new entries from {file_name}.")

                # Move out of ingress to prevent reprocessing loops
                shutil.move(file_path, PROCESSED_DIR / file_name)
            except Exception:
                conn.rollback()
                raise
            
        except FileNotFoundError as exc:
            print(f"Skipping {file_name}: file disappeared before processing ({exc}).")
        except pd.errors.EmptyDataError:
            print(f"Skipping {file_name}: file is empty.")
        except pd.errors.ParserError as exc:
            print(f"Skipping {file_name}: malformed CSV ({exc}).")
        except ValueError as exc:
            print(f"Skipping {file_name}: invalid data ({exc}).")
        except sqlite3.Error as exc:
            print(f"Failed to process file {file_name} due to database error: {exc}")
        except Exception as exc:
            print(f"Failed to process file {file_name} due to unexpected error: {exc}")
            
    conn.close()

def is_safe_file_name(file_name):
    return (
        isinstance(file_name, str)
        and file_name.endswith(".csv")
        and file_name == Path(file_name).name
        and ".." not in file_name
        and "/" not in file_name
        and "\\" not in file_name
    )

def validate_dataframe(df, file_name):
    required_cols = {"Date", "Description", "Amount"}
    if df.empty:
        raise ValueError("File is empty")
    if not required_cols.issubset(df.columns):
        raise ValueError("Missing expected headers")

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")

    if df["Date"].isna().any() or df["Amount"].isna().any():
        raise ValueError("Invalid date or amount values")

    return df

if __name__ == "__main__":
    process_new_statements()

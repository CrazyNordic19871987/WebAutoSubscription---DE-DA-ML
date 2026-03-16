"""
СберАвтоподписка - Финальная работа
Часть 3: Data Engineer (База данных + Пайплайн)
"""

import sqlite3
import pandas as pd
import json
import os
import shutil
from datetime import datetime

# Paths
DATA_PATH = r'C:\Users\admin\Desktop'
OUTPUT_PATH = r'C:\Users\admin\Desktop\SberAutoSubscription\de'
NEW_DATA_PATH = os.path.join(OUTPUT_PATH, 'new_data')
DB_PATH = os.path.join(OUTPUT_PATH, 'sberautopodpiska.db')

print("=" * 60)
print("Часть 3: Data Engineer")
print("=" * 60)

# Create directories
os.makedirs(NEW_DATA_PATH, exist_ok=True)
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ============================================================================
# 1. CREATE DATABASE SCHEMA
# ============================================================================
print("\n[1] Создание базы данных...")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create sessions table
cursor.execute("""
CREATE TABLE IF NOT EXISTS ga_sessions (
    session_id TEXT PRIMARY KEY,
    client_id REAL,
    visit_date TEXT,
    visit_time TEXT,
    visit_number INTEGER,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    utm_adcontent TEXT,
    utm_keyword TEXT,
    device_category TEXT,
    device_os TEXT,
    device_brand TEXT,
    device_model TEXT,
    device_screen_resolution TEXT,
    device_browser TEXT,
    geo_country TEXT,
    geo_city TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
""")

# Create hits table
cursor.execute("""
CREATE TABLE IF NOT EXISTS ga_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT,
    hit_date TEXT,
    hit_time REAL,
    hit_number INTEGER,
    hit_type TEXT,
    hit_referer TEXT,
    hit_page_path TEXT,
    event_category TEXT,
    event_action TEXT,
    event_label TEXT,
    event_value REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES ga_sessions(session_id)
)
""")

# Create indexes
cursor.execute("CREATE INDEX IF NOT EXISTS idx_hits_session ON ga_hits(session_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_date ON ga_sessions(visit_date)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_source ON ga_sessions(utm_source)")

conn.commit()
print(f"  База данных создана: {DB_PATH}")

# ============================================================================
# 2. LOAD INITIAL DATA
# ============================================================================
print("\n[2] Загрузка начальных данных...")

# Load sessions in chunks (to handle large file)
print("  Загрузка ga_sessions...")
chunks = pd.read_csv(
    os.path.join(DATA_PATH, 'ga_sessions.csv'),
    low_memory=False,
    chunksize=50000
)

for i, chunk in enumerate(chunks):
    chunk.to_sql('ga_sessions', conn, if_exists='append', index=False)
    print(f"    chunk {i+1}: {len(chunk):,} rows")

# Get sessions count
cursor.execute("SELECT COUNT(*) FROM ga_sessions")
sessions_count = cursor.fetchone()[0]
print(f"  Всего сессий в БД: {sessions_count:,}")

# Load hits
print("  Загрузка ga_hits...")
hits_chunks = pd.read_csv(
    os.path.join(DATA_PATH, 'ga_hits.csv'),
    low_memory=False,
    chunksize=100000
)

for i, chunk in enumerate(hits_chunks):
    chunk.to_sql('ga_hits', conn, if_exists='append', index=False)
    if (i+1) % 10 == 0:
        print(f"    chunk {i+1}: {len(chunk):,} rows")

cursor.execute("SELECT COUNT(*) FROM ga_hits")
hits_count = cursor.fetchone()[0]
print(f"  Всего хитов в БД: {hits_count:,}")

conn.close()
print("  Данные загружены!")

# ============================================================================
# 3. PIPELINE FOR NEW DATA
# ============================================================================
print("\n[3] Создание пайплайна для новых данных...")

def process_new_json(file_path):
    """Обработка нового JSON файла с данными"""
    
    print(f"\n  Обработка файла: {file_path}")
    
    # Load JSON
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Connect to DB
    conn = sqlite3.connect(DB_PATH)
    
    # Process sessions
    sessions_data = data.get('sessions', [])
    if sessions_data:
        sessions_df = pd.DataFrame(sessions_data)
        sessions_df.to_sql('ga_sessions', conn, if_exists='append', index=False)
        print(f"    Добавлено сессий: {len(sessions_data)}")
    
    # Process hits
    hits_data = data.get('hits', [])
    if hits_data:
        hits_df = pd.DataFrame(hits_data)
        hits_df.to_sql('ga_hits', conn, if_exists='append', index=False)
        print(f"    Добавлено хитов: {len(hits_data)}")
    
    conn.close()
    
    # Move processed file to archive
    archive_path = os.path.join(OUTPUT_PATH, 'archive', os.path.basename(file_path))
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    shutil.move(file_path, archive_path)
    print(f"    Перемещено в архив: {archive_path}")
    
    return True

def check_and_process_new_files():
    """Проверка и обработка новых файлов"""
    
    new_files = [f for f in os.listdir(NEW_DATA_PATH) if f.endswith('.json')]
    
    if not new_files:
        print("    Новых файлов нет")
        return
    
    for file_name in new_files:
        file_path = os.path.join(NEW_DATA_PATH, file_name)
        try:
            process_new_json(file_path)
        except Exception as e:
            print(f"    Ошибка обработки {file_name}: {e}")

# Create example new data file
example_new_data = {
    "sessions": [
        {
            "session_id": "new_session_001",
            "client_id": 123456789.0,
            "visit_date": "2022-01-15",
            "visit_time": "14:30:00",
            "visit_number": 1,
            "utm_source": "new_source",
            "utm_medium": "organic",
            "utm_campaign": "new_campaign",
            "device_category": "desktop",
            "geo_country": "Russia",
            "geo_city": "Moscow"
        }
    ],
    "hits": [
        {
            "session_id": "new_session_001",
            "hit_date": "2022-01-15",
            "hit_time": 14.5,
            "hit_number": 1,
            "hit_type": "event",
            "event_action": "sub_car_claim_click"
        }
    ]
}

# Save example
example_file = os.path.join(NEW_DATA_PATH, 'example_new_data.json')
with open(example_file, 'w', encoding='utf-8') as f:
    json.dump(example_new_data, f, ensure_ascii=False)

print(f"  Пример файла создан: {example_file}")
print("\n  Для тестирования пайплайна:")
print("    1. Положите .json файл в папку new_data/")
print("    2. Запустите: python -c \"from de_pipeline import check_and_process_new_files; check_and_process_new_files()\"")

# ============================================================================
# 4. VERIFY DATABASE
# ============================================================================
print("\n[4] Проверка базы данных...")

conn = sqlite3.connect(DB_PATH)

# Show tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print(f"  Таблицы: {[t[0] for t in tables]}")

# Show sample data
print("\n  Sample из ga_sessions:")
cursor.execute("SELECT session_id, visit_date, device_category, geo_city FROM ga_sessions LIMIT 3")
for row in cursor.fetchall():
    print(f"    {row}")

print("\n  Sample из ga_hits:")
cursor.execute("SELECT session_id, event_action, hit_date FROM ga_hits LIMIT 3")
for row in cursor.fetchall():
    print(f"    {row}")

conn.close()

# Save schema to file
schema_sql = """
-- Схема базы данных СберАвтоподписка

-- Таблица сессий
CREATE TABLE ga_sessions (
    session_id TEXT PRIMARY KEY,
    client_id REAL,
    visit_date TEXT,
    visit_time TEXT,
    visit_number INTEGER,
    utm_source TEXT,
    utm_medium TEXT,
    utm_campaign TEXT,
    utm_adcontent TEXT,
    utm_keyword TEXT,
    device_category TEXT,
    device_os TEXT,
    device_brand TEXT,
    device_model TEXT,
    device_screen_resolution TEXT,
    device_browser TEXT,
    geo_country TEXT,
    geo_city TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица хитов
CREATE TABLE ga_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT,
    hit_date TEXT,
    hit_time REAL,
    hit_number INTEGER,
    hit_type TEXT,
    hit_referer TEXT,
    hit_page_path TEXT,
    event_category TEXT,
    event_action TEXT,
    event_label TEXT,
    event_value REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES ga_sessions(session_id)
);

-- Индексы
CREATE INDEX idx_hits_session ON ga_hits(session_id);
CREATE INDEX idx_sessions_date ON ga_sessions(visit_date);
CREATE INDEX idx_sessions_source ON ga_sessions(utm_source);
"""

with open(os.path.join(OUTPUT_PATH, 'schema.sql'), 'w', encoding='utf-8') as f:
    f.write(schema_sql)

print("\n" + "=" * 60)
print("Часть 3 (Data Engineer) ЗАВЕРШЕНА")
print("=" * 60)
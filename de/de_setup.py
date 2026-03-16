"""
СберАвтоподписка - Data Engineer (оптимизированная версия)
Создание БД и пайплайна
"""

import sqlite3
import json
import os
import shutil
from datetime import datetime

OUTPUT_PATH = r'C:\Users\admin\Desktop\SberAutoSubscription\de'
NEW_DATA_PATH = os.path.join(OUTPUT_PATH, 'new_data')
DB_PATH = os.path.join(OUTPUT_PATH, 'sberautopodpiska.db')

print("=" * 60)
print("Data Engineer - Создание БД и пайплайна")
print("=" * 60)

os.makedirs(NEW_DATA_PATH, exist_ok=True)

# ============================================================================
# 1. CREATE DATABASE SCHEMA
# ============================================================================
print("\n[1] Создание схемы базы данных...")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Sessions table
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

# Hits table
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

# Indexes
cursor.execute("CREATE INDEX IF NOT EXISTS idx_hits_session ON ga_hits(session_id)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_date ON ga_sessions(visit_date)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_source ON ga_sessions(utm_source)")

conn.commit()
print(f"  База данных создана: {DB_PATH}")

# ============================================================================
# 2. CREATE PIPELINE SCRIPT
# ============================================================================
pipeline_code = '''"""
Пайпайн для обработки новых JSON данных СберАвтоподписка
Использование:
    1. Положите .json файл в папку new_data/
    2. Запустите: python de_run_pipeline.py
"""

import sqlite3
import json
import os
import shutil
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), 'sberautopodpiska.db')
NEW_DATA_PATH = os.path.join(os.path.dirname(__file__), 'new_data')
ARCHIVE_PATH = os.path.join(os.path.dirname(__file__), 'archive')

def process_json_file(file_path):
    """Обработка одного JSON файла"""
    print(f"Обработка: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Sessions
    sessions = data.get('sessions', [])
    if sessions:
        for s in sessions:
            cols = ', '.join(s.keys())
            placeholders = ', '.join(['?'] * len(s))
            vals = list(s.values())
            try:
                cursor.execute(f"INSERT OR IGNORE INTO ga_sessions ({cols}) VALUES ({placeholders})", vals)
            except Exception as e:
                pass  # Skip duplicates
        
    # Hits
    hits = data.get('hits', [])
    if hits:
        for h in hits:
            cols = ', '.join(h.keys())
            placeholders = ', '.join(['?'] * len(h))
            vals = list(h.values())
            cursor.execute(f"INSERT INTO ga_hits ({cols}) VALUES ({placeholders})", vals)
    
    conn.commit()
    conn.close()
    
    # Move to archive
    os.makedirs(ARCHIVE_PATH, exist_ok=True)
    shutil.move(file_path, os.path.join(ARCHIVE_PATH, os.path.basename(file_path)))
    print(f"  Готово! Сессий: {len(sessions)}, Хитов: {len(hits)}")

def run_pipeline():
    """Запуск пайплайна"""
    files = [f for f in os.listdir(NEW_DATA_PATH) if f.endswith('.json')]
    
    if not files:
        print("Новых файлов нет")
        return
    
    for f in files:
        process_json_file(os.path.join(NEW_DATA_PATH, f))

if __name__ == '__main__':
    run_pipeline()
'''

with open(os.path.join(OUTPUT_PATH, 'de_run_pipeline.py'), 'w', encoding='utf-8') as f:
    f.write(pipeline_code)

print("\n[2] Пайпайн создан: de_run_pipeline.py")

# ============================================================================
# 3. EXAMPLE DATA FILE
# ============================================================================
example_data = {
    "sessions": [
        {
            "session_id": "test_001",
            "client_id": 123456.0,
            "visit_date": "2022-01-15",
            "visit_time": "14:30:00",
            "visit_number": 1,
            "utm_source": "test_source",
            "utm_medium": "organic",
            "utm_campaign": "test_campaign",
            "device_category": "desktop",
            "geo_country": "Russia",
            "geo_city": "Moscow"
        }
    ],
    "hits": [
        {
            "session_id": "test_001",
            "hit_date": "2022-01-15",
            "hit_time": 14.5,
            "hit_number": 1,
            "hit_type": "event",
            "event_action": "sub_car_claim_click"
        }
    ]
}

with open(os.path.join(NEW_DATA_PATH, 'example.json'), 'w', encoding='utf-8') as f:
    json.dump(example_data, f, ensure_ascii=False, indent=2)

print(f"  Пример данных: {NEW_DATA_PATH}/example.json")

# ============================================================================
# 4. VERIFY
# ============================================================================
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [t[0] for t in cursor.fetchall()]
print(f"\n[3] Таблицы в БД: {tables}")

conn.close()

print("\n" + "=" * 60)
print("Data Engineer готов!")
print(f"База данных: {DB_PATH}")
print("=" * 60)
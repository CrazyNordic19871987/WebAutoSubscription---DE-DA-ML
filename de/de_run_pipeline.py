"""
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

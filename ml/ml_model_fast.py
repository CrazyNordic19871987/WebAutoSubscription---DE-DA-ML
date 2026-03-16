"""
СберАвтоподписка - ML (оптимизированная версия)
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
import pickle
import os

DATA_PATH = r'C:\Users\admin\Desktop'
OUTPUT_PATH = r'C:\Users\admin\Desktop\SberAutoSubscription\ml'

TARGET_ACTIONS = ['sub_car_claim_click', 'sub_car_claim_submit_click',
    'sub_open_dialog_click', 'sub_custom_question_submit_click',
    'sub_call_number_click', 'sub_callback_submit_click',
    'sub_submit_success', 'sub_car_request_submit_click']

print("[1] Загрузка данных (сэмплирование)...")

# Load sessions with sampling for speed
sessions = pd.read_csv(os.path.join(DATA_PATH, 'ga_sessions.csv'), low_memory=False)
print(f"  Всего сессий: {len(sessions):,}")

# Sample 200k for training (faster)
sessions_sample = sessions.sample(n=200000, random_state=42).copy()
print(f"  Сэмпл: {len(sessions_sample):,}")

# Load hits - only target actions for efficiency  
print("  Загрузка целевых действий...")
hits = pd.read_csv(os.path.join(DATA_PATH, 'ga_hits.csv'), 
                   low_memory=False, usecols=['session_id', 'event_action'])
hits['is_target'] = hits['event_action'].isin(TARGET_ACTIONS)
target_sessions = hits[hits['is_target']]['session_id'].unique()
print(f"  Целевых сессий: {len(target_sessions):,}")

# Create target
sessions_sample['target'] = sessions_sample['session_id'].isin(target_sessions).astype(int)
print(f"  Target distribution: {sessions_sample['target'].value_counts().to_dict()}")

print("\n[2] Feature Engineering...")

# Time features
sessions_sample['visit_date'] = pd.to_datetime(sessions_sample['visit_date'])
sessions_sample['visit_hour'] = pd.to_datetime(sessions_sample['visit_time'], format='%H:%M:%S', errors='coerce').dt.hour.fillna(12)
sessions_sample['visit_dayofweek'] = sessions_sample['visit_date'].dt.dayofweek

# Binary features
sessions_sample['is_organic'] = sessions_sample['utm_medium'].isin(['organic', 'referral', '(none)']).astype(int)
sessions_sample['is_mobile'] = (sessions_sample['device_category'] == 'mobile').astype(int)
sessions_sample['is_desktop'] = (sessions_sample['device_category'] == 'desktop').astype(int)
sessions_sample['is_first_visit'] = (sessions_sample['visit_number'] == 1).astype(int)

# Encode categoricals
cat_cols = ['device_category', 'utm_medium', 'utm_source', 'geo_country', 'geo_city']
for col in cat_cols:
    le = LabelEncoder()
    sessions_sample[col + '_enc'] = le.fit_transform(sessions_sample[col].fillna('unknown').astype(str))

print("\n[3] Подготовка данных...")
feature_cols = ['visit_number', 'visit_hour', 'visit_dayofweek', 'is_organic', 
                'is_mobile', 'is_desktop', 'is_first_visit',
                'device_category_enc', 'utm_medium_enc', 'utm_source_enc', 
                'geo_country_enc', 'geo_city_enc']

X = sessions_sample[feature_cols].fillna(0)
y = sessions_sample['target']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"  Train: {len(X_train):,}, Test: {len(X_test):,}")

print("\n[4] Обучение Random Forest...")
model = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42, n_jobs=-1, class_weight='balanced')
model.fit(X_train, y_train)

pred = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, pred)
print(f"  ROC-AUC: {auc:.4f}")

# Feature importance
print("\n[5] Важность фичей:")
importance = pd.DataFrame({'feature': feature_cols, 'importance': model.feature_importances_})
importance = importance.sort_values('importance', ascending=False)
print(importance.to_string(index=False))

print("\n[6] Сохранение модели...")
with open(os.path.join(OUTPUT_PATH, 'model.pkl'), 'wb') as f:
    pickle.dump({'model': model, 'features': feature_cols, 'auc': auc}, f)

print(f"\nМодель сохранена! ROC-AUC: {auc:.4f}")
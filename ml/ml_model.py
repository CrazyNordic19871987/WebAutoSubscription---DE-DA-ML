"""
СберАвтоподписка - Финальная работа
Часть 2: ML Engineer (Предиктивная модель + API сервис)
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, classification_report, confusion_matrix
import pickle
import os
import warnings

warnings.filterwarnings('ignore')

DATA_PATH = r'C:\Users\admin\Desktop'
OUTPUT_PATH = r'C:\Users\admin\Desktop\SberAutoSubscription\ml'

# Target actions
TARGET_ACTIONS = [
    'sub_car_claim_click', 'sub_car_claim_submit_click',
    'sub_open_dialog_click', 'sub_custom_question_submit_click',
    'sub_call_number_click', 'sub_callback_submit_click',
    'sub_submit_success', 'sub_car_request_submit_click'
]

print("=" * 60)
print("Часть 2: ML Engineer")
print("=" * 60)

# ============================================================================
# 1. DATA LOADING AND FEATURE ENGINEERING
# ============================================================================
print("\n[1] Загрузка данных и Feature Engineering...")

# Load sessions
sessions = pd.read_csv(
    os.path.join(DATA_PATH, 'ga_sessions.csv'),
    low_memory=False
)
print(f"  Sessions: {sessions.shape[0]:,} rows")

# Load hits (minimal columns for memory efficiency)
hits = pd.read_csv(
    os.path.join(DATA_PATH, 'ga_hits.csv'),
    low_memory=False,
    usecols=['session_id', 'event_action', 'hit_page_path']
)
print(f"  Hits: {hits.shape[0]:,} rows")

# Create target variable
print("\n[2] Создание целевой переменной...")
hits['is_target'] = hits['event_action'].isin(TARGET_ACTIONS)
target_by_session = hits.groupby('session_id')['is_target'].any().reset_index()
target_by_session.columns = ['session_id', 'target']

data = sessions.merge(target_by_session, on='session_id', how='left')
data['target'] = data['target'].fillna(False).astype(int)
print(f"  Целевых действий: {data['target'].sum():,} ({data['target'].mean()*100:.2f}%)")

# ============================================================================
# 2. FEATURE ENGINEERING
# ============================================================================
print("\n[3] Feature Engineering...")

# Time features
data['visit_date'] = pd.to_datetime(data['visit_date'])
data['visit_hour'] = pd.to_datetime(data['visit_time'], format='%H:%M:%S', errors='coerce').dt.hour.fillna(12)
data['visit_dayofweek'] = data['visit_date'].dt.dayofweek
data['visit_month'] = data['visit_date'].dt.month
data['is_weekend'] = (data['visit_dayofweek'] >= 5).astype(int)

# Traffic features
organic_mediums = ['organic', 'referral', '(none)']
data['is_organic'] = data['utm_medium'].isin(organic_mediums).astype(int)

# Social media sources
social_sources = ['QxAxdyPLuQMEcrdZWdWb', 'MvfHsxITijuriZxsqZqt', 
                  'ISrKoXQCxqqYvAZICvjs', 'IZEXUFLARCUMynmHNBGo',
                  'PlbkrSYoHuZBWfYjYnfw', 'gVRrcxiDQubJiljoTbGm']
data['is_social'] = data['utm_source'].isin(social_sources).astype(int)

# Presence cities
presence_cities = ['Moscow', 'Moscow Oblast', 'Saint Petersburg', 'St. Petersburg']
data['is_presence_city'] = data['geo_city'].isin(presence_cities).astype(int)

# Device features
data['is_mobile'] = (data['device_category'] == 'mobile').astype(int)
data['is_desktop'] = (data['device_category'] == 'desktop').astype(int)

# Campaign features
data['has_campaign'] = data['utm_campaign'].notna().astype(int)
data['has_keyword'] = data['utm_keyword'].notna().astype(int)

# Visit number features
data['is_first_visit'] = (data['visit_number'] == 1).astype(int)
data['visit_number_log'] = np.log1p(data['visit_number'])

# Feature from hits - count events per session
event_counts = hits.groupby('session_id').size().reset_index(name='total_events')
target_counts = hits[hits['is_target']].groupby('session_id').size().reset_index(name='target_events')
event_counts = event_counts.merge(target_counts, on='session_id', how='left')
event_counts['target_events'] = event_counts['target_events'].fillna(0)

data = data.merge(event_counts, on='session_id', how='left')
data['total_events'] = data['total_events'].fillna(0)
data['target_events'] = data['target_events'].fillna(0)
data['has_viewed_car'] = (data['total_events'] > 0).astype(int)

print(f"  Создано новых фичей: 15+")

# ============================================================================
# 3. PREPARE FEATURES FOR MODEL
# ============================================================================
print("\n[4] Подготовка фичей...")

# Select features for model
feature_cols = [
    'visit_number', 'visit_number_log', 'visit_hour', 'visit_dayofweek', 
    'visit_month', 'is_weekend', 'is_organic', 'is_social', 
    'is_presence_city', 'is_mobile', 'is_desktop', 'has_campaign', 
    'has_keyword', 'is_first_visit', 'total_events'
]

# Add categorical features with encoding
cat_features = ['device_category', 'utm_medium', 'utm_source', 'geo_country', 'geo_city']

# Encode categorical features
label_encoders = {}
for col in cat_features:
    data[col + '_encoded'] = data[col].fillna('unknown').astype(str)
    le = LabelEncoder()
    data[col + '_encoded'] = le.fit_transform(data[col + '_encoded'])
    label_encoders[col] = le
    feature_cols.append(col + '_encoded')

# Prepare final feature matrix
X = data[feature_cols].fillna(0)
y = data['target']

print(f"  Размерность X: {X.shape}")
print(f"  Фичи: {feature_cols}")

# Split data
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
print(f"  Train: {X_train.shape[0]:,}, Test: {X_test.shape[0]:,}")

# ============================================================================
# 4. TRAIN MODELS
# ============================================================================
print("\n[5] Обучение моделей...")

# Baseline: Logistic Regression
print("  Logistic Regression...")
lr = LogisticRegression(max_iter=1000, random_state=42, n_jobs=-1)
lr.fit(X_train, y_train)
lr_pred = lr.predict_proba(X_test)[:, 1]
lr_auc = roc_auc_score(y_test, lr_pred)
print(f"    ROC-AUC: {lr_auc:.4f}")

# Model 1: Random Forest
print("  Random Forest...")
rf = RandomForestClassifier(
    n_estimators=100, max_depth=10, random_state=42, n_jobs=-1, 
    class_weight='balanced'
)
rf.fit(X_train, y_train)
rf_pred = rf.predict_proba(X_test)[:, 1]
rf_auc = roc_auc_score(y_test, rf_pred)
print(f"    ROC-AUC: {rf_auc:.4f}")

# Model 2: Gradient Boosting (smaller for speed)
print("  Gradient Boosting...")
gb = GradientBoostingClassifier(
    n_estimators=100, max_depth=5, random_state=42, learning_rate=0.1
)
gb.fit(X_train, y_train)
gb_pred = gb.predict_proba(X_test)[:, 1]
gb_auc = roc_auc_score(y_test, gb_pred)
print(f"    ROC-AUC: {gb_auc:.4f}")

# Select best model
models = {'LogisticRegression': (lr, lr_auc), 'RandomForest': (rf, rf_auc), 'GradientBoosting': (gb, gb_auc)}
best_model_name = max(models, key=lambda x: models[x][1])
best_model, best_auc = models[best_model_name]
print(f"\n  Лучшая модель: {best_model_name} (ROC-AUC: {best_auc:.4f})")

# ============================================================================
# 5. EVALUATE BEST MODEL
# ============================================================================
print("\n[6] Оценка лучшей модели...")
print(classification_report(y_test, (best_model.predict_proba(X_test)[:, 1] > 0.5).astype(int)))

# Feature importance
if hasattr(best_model, 'feature_importances_'):
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': best_model.feature_importances_
    }).sort_values('importance', ascending=False)
    print("\nТоп-10 важных фичей:")
    print(importance.head(10).to_string(index=False))

# ============================================================================
# 6. SAVE MODEL
# ============================================================================
print("\n[7] Сохранение модели...")

# Save model
model_data = {
    'model': best_model,
    'feature_cols': feature_cols,
    'label_encoders': label_encoders,
    'auc': best_auc
}

with open(os.path.join(OUTPUT_PATH, 'model.pkl'), 'wb') as f:
    pickle.dump(model_data, f)
print(f"  Модель сохранена: model.pkl")

# Save preprocessing info
preprocessing_info = {
    'target_actions': TARGET_ACTIONS,
    'organic_mediums': organic_mediums,
    'social_sources': social_sources,
    'presence_cities': presence_cities
}
with open(os.path.join(OUTPUT_PATH, 'preprocessing_info.pkl'), 'wb') as f:
    pickle.dump(preprocessing_info, f)
print(f"  Инфо сохранено: preprocessing_info.pkl")

print("\n" + "=" * 60)
print("Часть 2 (ML) ЗАВЕРШЕНА")
print(f"Итоговый ROC-AUC: {best_auc:.4f}")
print("=" * 60)
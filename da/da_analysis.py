"""
СберАвтоподписка - Финальная работа
Часть 1: Data Analyst (EDA + Гипотезы + Бизнес-вопросы)

Автор: Data Science Student
Дата: 2024
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import warnings
import os

warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-whitegrid')

# Paths
DATA_PATH = r'C:\Users\admin\Desktop'
OUTPUT_PATH = r'C:\Users\admin\Desktop\SberAutoSubscription\da'

# Target actions (from coursework definition)
TARGET_ACTIONS = [
    'sub_car_claim_click',
    'sub_car_claim_submit_click',
    'sub_open_dialog_click',
    'sub_custom_question_submit_click',
    'sub_call_number_click',
    'sub_callback_submit_click',
    'sub_submit_success',
    'sub_car_request_submit_click'
]

# Organic traffic mediums
ORGANIC_MEDIUMS = ['organic', 'referral', '(none)']

# Social media sources
SOCIAL_SOURCES = [
    'QxAxdyPLuQMEcrdZWdWb',
    'MvfHsxITijuriZxsqZqt',
    'ISrKoXQCxqqYvAZICvjs',
    'IZEXUFLARCUMynmHNBGo',
    'PlbkrSYoHuZBWfYjYnfw',
    'gVRrcxiDQubJiljoTbGm'
]

# Presence cities
PRESENCE_CITIES = ['Moscow', 'Moscow Oblast', 'Saint Petersburg', 'St. Petersburg']

print("=" * 60)
print("СберАвтоподписка - Анализ данных")
print("Часть 1: Data Analyst")
print("=" * 60)

# ============================================================================
# 1. DATA LOADING AND BASIC CLEANING
# ============================================================================
print("\n[1] Загрузка данных...")

# Load sessions
print("  Загрузка ga_sessions...")
sessions = pd.read_csv(
    os.path.join(DATA_PATH, 'ga_sessions.csv'),
    low_memory=False
)
print(f"    Sessions: {sessions.shape[0]:,} rows, {sessions.shape[1]} columns")

# Load hits with optimized types
print("  Загрузка ga_hits (может занять время)...")
hits = pd.read_csv(
    os.path.join(DATA_PATH, 'ga_hits.csv'),
    low_memory=False,
    usecols=['session_id', 'hit_date', 'hit_time', 'event_action', 'hit_page_path']
)
print(f"    Hits: {hits.shape[0]:,} rows")

# Basic cleaning
print("\n[2] Базовая чистка данных...")

# Check for duplicates in sessions
session_dups = sessions.duplicated(subset=['session_id']).sum()
print(f"  Дубликатов session_id в sessions: {session_dups:,}")
if session_dups > 0:
    sessions = sessions.drop_duplicates(subset=['session_id'])
    print("    Дубликаты удалены")

# Check for missing values
print("\n  Пропущенные значения в sessions:")
missing_pct = (sessions.isnull().sum() / len(sessions) * 100).round(2)
print(missing_pct[missing_pct > 0])

# Mark target action presence per session
print("\n[3] Определение целевых действий...")
hits['is_target_action'] = hits['event_action'].isin(TARGET_ACTIONS)
target_by_session = hits.groupby('session_id')['is_target_action'].any().reset_index()
target_by_session.columns = ['session_id', 'has_target_action']

# Merge with sessions
data = sessions.merge(target_by_session, on='session_id', how='left')
data['has_target_action'] = data['has_target_action'].fillna(False)

print(f"  Сессий с целевым действием: {data['has_target_action'].sum():,} ({data['has_target_action'].mean()*100:.2f}%)")

# Calculate CR (Conversion Rate)
CR = data['has_target_action'].mean()
print(f"  Общий CR: {CR*100:.4f}%")

# ============================================================================
# 2. EDA - EXPLORATORY DATA ANALYSIS
# ============================================================================
print("\n" + "=" * 60)
print("РАЗВЕДОЧНЫЙ АНАЛИЗ ДАННЫХ (EDA)")
print("=" * 60)

# 2.1 Visit distribution over time
print("\n[2.1] Временное распределение визитов...")
data['visit_date'] = pd.to_datetime(data['visit_date'])
data['visit_month'] = data['visit_date'].dt.to_period('M')

visits_by_month = data.groupby('visit_month').size()
print(visits_by_month)

# 2.2 Device categories
print("\n[2.2] Устройства...")
device_dist = data['device_category'].value_counts()
print(device_dist)

# Визуализация распределений
print("\n[2.2.1] Визуализации распределений...")
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Распределение по устройствам (bar chart)
device_dist.plot(kind='bar', ax=axes[0, 0], color=['#3498db', '#2ecc71', '#e74c3c'])
axes[0, 0].set_title('Распределение визитов по устройствам')
axes[0, 0].set_xlabel('Устройство')
axes[0, 0].set_ylabel('Количество визитов')
axes[0, 0].tick_params(axis='x', rotation=0)

# Распределение по типу трафика (pie chart)
traffic_medium = data['utm_medium'].value_counts().head(6)
traffic_medium.plot(kind='pie', ax=axes[0, 1], autopct='%1.1f%%', startangle=90)
axes[0, 1].set_title('Распределение по типам трафика (топ-6)')
axes[0, 1].set_ylabel('')

# Boxplot - CR по устройствам
cr_by_device_viz = data.groupby('device_category')['has_target_action'].mean() * 100
cr_by_device_viz.plot(kind='bar', ax=axes[1, 0], color=['#3498db', '#2ecc71', '#e74c3c'])
axes[1, 0].set_title('CR по устройствам (%)')
axes[1, 0].set_xlabel('Устройство')
axes[1, 0].set_ylabel('Конверсия (%)')
axes[1, 0].tick_params(axis='x', rotation=0)

# Гистограмма визитов по месяцам
visits_by_month.plot(kind='bar', ax=axes[1, 1], color='#9b59b6')
axes[1, 1].set_title('Визиты по месяцам')
axes[1, 1].set_xlabel('Месяц')
axes[1, 1].set_ylabel('Количество визитов')
axes[1, 1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_PATH, 'eda_visualizations.png'), dpi=150, bbox_inches='tight')
plt.close()
print(f"  Сохранено: eda_visualizations.png")

# 2.3 Traffic sources
print("\n[2.3] Источники трафика...")
traffic_by_source = data['utm_source'].value_counts().head(10)
print(traffic_by_source)

# 2.4 Traffic mediums
print("\n[2.4] Типы трафика...")
traffic_by_medium = data['utm_medium'].value_counts()
print(traffic_by_medium)

# 2.5 Geography
print("\n[2.5] География...")
geo_dist = data['geo_city'].value_counts().head(15)
print(geo_dist)

# 2.6 CR by device
print("\n[2.6] CR по устройствам...")
cr_by_device = data.groupby('device_category')['has_target_action'].agg(['mean', 'count'])
cr_by_device.columns = ['CR', 'visits']
cr_by_device['CR'] = cr_by_device['CR'] * 100
print(cr_by_device)

# 2.7 CR by traffic type (organic vs paid)
print("\n[2.7] CR по типу трафика...")
data['is_organic'] = data['utm_medium'].isin(ORGANIC_MEDIUMS)
cr_by_traffic = data.groupby('is_organic')['has_target_action'].agg(['mean', 'count'])
cr_by_traffic.columns = ['CR', 'visits']
cr_by_traffic.index = ['Платный', 'Органический']
cr_by_traffic['CR'] = cr_by_traffic['CR'] * 100
print(cr_by_traffic)

# 2.8 CR by city
print("\n[2.8] CR по городам...")
data['is_presence_city'] = data['geo_city'].isin(PRESENCE_CITIES)
cr_by_city = data.groupby('is_presence_city')['has_target_action'].agg(['mean', 'count'])
cr_by_city.columns = ['CR', 'visits']
cr_by_city.index = ['Другие регионы', 'Города присутствия']
cr_by_city['CR'] = cr_by_city['CR'] * 100
print(cr_by_city)

# 2.9 Корреляционный анализ (Heatmap)
print("\n[2.9] Корреляционный анализ...")
corr_data = data.copy()
corr_data['device_cat_num'] = corr_data['device_category'].map({'desktop': 0, 'mobile': 1, 'tablet': 2})
corr_data['is_organic_num'] = corr_data['is_organic'].astype(int)
corr_data['is_presence_city_num'] = corr_data['is_presence_city'].astype(int)
corr_data['has_target_num'] = corr_data['has_target_action'].astype(int)

corr_cols = ['visit_number', 'device_cat_num', 'is_organic_num', 'is_presence_city_num', 'has_target_num']
corr_matrix = corr_data[corr_cols].corr()

plt.figure(figsize=(10, 8))
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, fmt='.3f', 
            xticklabels=['visit_number', 'device', 'organic', 'presence_city', 'target'],
            yticklabels=['visit_number', 'device', 'organic', 'presence_city', 'target'])
plt.title('Корреляционная матрица')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_PATH, 'eda_correlation_heatmap.png'), dpi=150, bbox_inches='tight')
plt.close()
print("  Сохранено: eda_correlation_heatmap.png")

# Выводы по корреляциям
print("\n  Выводы по корреляциям:")
print(f"    - visit_number и target: {corr_matrix.loc['visit_number', 'has_target_num']:.4f}")
print(f"    - device и target: {corr_matrix.loc['device_cat_num', 'has_target_num']:.4f}")
print(f"    - is_organic и target: {corr_matrix.loc['is_organic_num', 'has_target_num']:.4f}")

# ============================================================================
# 3. HYPOTHESIS TESTING
# ============================================================================
print("\n" + "=" * 60)
print("ПРОВЕРКА ГИПОТЕЗ")
print("=" * 60)

# Function for hypothesis testing using chi-square
def test_hypothesis(data, group_col, group1_name, group2_name, alpha=0.05):
    """Test hypothesis about CR difference between two groups"""
    
    # Get conversion data for both groups
    group1_data = data[data[group_col] == True]['has_target_action']
    group2_data = data[data[group_col] == False]['has_target_action']
    
    # Calculate CRs
    cr1 = group1_data.mean()
    cr2 = group2_data.mean()
    n1 = len(group1_data)
    n2 = len(group2_data)
    
    # Create contingency table
    table = np.array([
        [group1_data.sum(), n1 - group1_data.sum()],
        [group2_data.sum(), n2 - group2_data.sum()]
    ])
    
    # Chi-square test
    chi2, p_value, dof, expected = stats.chi2_contingency(table)
    
    # Effect size (Cramers V)
    n = table.sum()
    cramers_v = np.sqrt(chi2 / (n * (min(table.shape) - 1)))
    
    print(f"\nГипотеза: CR {group1_name} vs {group2_name}")
    print(f"  CR {group1_name}: {cr1*100:.4f}% (n={n1:,})")
    print(f"  CR {group2_name}: {cr2*100:.4f}% (n={n2:,})")
    print(f"  Chi-square: {chi2:.2f}, p-value: {p_value:.6f}")
    print(f"  Cramers V: {cramers_v:.4f}")
    
    if p_value < alpha:
        print(f"  Результат: ОТКЛОНЯЕМ нулевую гипотезу (p < {alpha})")
        print(f"  Вывод: Статистически значимая разница в CR между группами")
    else:
        print(f"  Результат: НЕ отклоняем нулевую гипотезу (p >= {alpha})")
        print(f"  Вывод: Нет статистически значимой разницы в CR между группами")
    
    return {
        'cr1': cr1,
        'cr2': cr2,
        'chi2': chi2,
        'p_value': p_value,
        'cramers_v': cramers_v,
        'significant': p_value < alpha
    }

# Hypothesis 1: Organic vs Paid traffic
print("\n" + "-" * 40)
print("ГИПОТЕЗА 1: Органический трафик не отличается от платного")
print("-" * 40)
result1 = test_hypothesis(data, 'is_organic', 'органический', 'платный')

# Hypothesis 2: Mobile vs Desktop
print("\n" + "-" * 40)
print("ГИПОТЕЗА 2: Мобильный трафик не отличается от десктопного")
print("-" * 40)
data['is_mobile'] = data['device_category'] == 'mobile'
result2 = test_hypothesis(data, 'is_mobile', 'мобильный', 'десктопный')

# Hypothesis 3: Presence cities vs Other regions
print("\n" + "-" * 40)
print("ГИПОТЕЗА 3: Трафик из городов присутствия не отличается от других регионов")
print("-" * 40)
result3 = test_hypothesis(data, 'is_presence_city', 'города присутствия', 'другие регионы')

# ============================================================================
# 4. BUSINESS QUESTIONS
# ============================================================================
print("\n" + "=" * 60)
print("ОТВЕТЫ НА ВОПРОСЫ БИЗНЕСА")
print("=" * 60)

# Question 1: Best sources/campaigns/devices/locations
print("\n[ВОПРОС 1] Из каких источников/кампаний/устройств/локаций самый целевой трафик?")
print("-" * 40)

# By source
print("\nПо источникам (топ-10 по объему и CR):")
source_analysis = data.groupby('utm_source').agg(
    visits=('session_id', 'count'),
    conversions=('has_target_action', 'sum')
).reset_index()
source_analysis['CR'] = source_analysis['conversions'] / source_analysis['visits'] * 100
source_analysis = source_analysis.sort_values('visits', ascending=False).head(10)
print(source_analysis[['utm_source', 'visits', 'conversions', 'CR']].to_string(index=False))

# By device
print("\nПо устройствам:")
device_analysis = data.groupby('device_category').agg(
    visits=('session_id', 'count'),
    conversions=('has_target_action', 'sum')
).reset_index()
device_analysis['CR'] = device_analysis['conversions'] / device_analysis['visits'] * 100
print(device_analysis[['device_category', 'visits', 'conversions', 'CR']].to_string(index=False))

# By city
print("\nПо городам (топ-10 по объему):")
city_analysis = data.groupby('geo_city').agg(
    visits=('session_id', 'count'),
    conversions=('has_target_action', 'sum')
).reset_index()
city_analysis['CR'] = city_analysis['conversions'] / city_analysis['visits'] * 100
city_analysis = city_analysis.sort_values('visits', ascending=False).head(10)
print(city_analysis[['geo_city', 'visits', 'conversions', 'CR']].to_string(index=False))

# By campaign
print("\nПо кампаниям (топ-10 по объему):")
campaign_analysis = data.groupby('utm_campaign').agg(
    visits=('session_id', 'count'),
    conversions=('has_target_action', 'sum')
).reset_index()
campaign_analysis['CR'] = campaign_analysis['conversions'] / campaign_analysis['visits'] * 100
campaign_analysis = campaign_analysis.sort_values('visits', ascending=False).head(10)
print(campaign_analysis[['utm_campaign', 'visits', 'conversions', 'CR']].to_string(index=False))

# Question 2: Car popularity and CR
print("\n[ВОПРОС 2] Какие авто пользуются наибольшим спросом?")
print("-" * 40)

# Extract car info from hit_page_path
# Note: Need to analyze which pages were visited
car_hits = hits[hits['hit_page_path'].notna()].copy()
# Looking for car card pages - typically contain car identifiers
car_hits['page_type'] = car_hits['hit_page_path'].apply(
    lambda x: 'car_card' if '/cars/' in str(x) or '/auto/' in str(x) else 'other'
)

car_visits = car_hits[car_hits['page_type'] == 'car_card']['session_id'].unique()
data['viewed_car'] = data['session_id'].isin(car_visits)

car_analysis = data[data['viewed_car']].groupby('utm_campaign').agg(
    visits=('session_id', 'count'),
    conversions=('has_target_action', 'sum')
).reset_index()
car_analysis['CR'] = car_analysis['conversions'] / car_analysis['visits'] * 100
car_analysis = car_analysis.sort_values('visits', ascending=False).head(10)
print("Кампании с наибольшим просмотром авто:")
print(car_analysis.to_string(index=False))

# Question 3: Social media presence
print("\n[ВОПРОС 3] Стоит ли увеличивать присутствие в соцсетях?")
print("-" * 40)

data['is_social'] = data['utm_source'].isin(SOCIAL_SOURCES)

social_analysis = data.groupby('is_social').agg(
    visits=('session_id', 'count'),
    conversions=('has_target_action', 'sum')
).reset_index()
social_analysis['CR'] = social_analysis['conversions'] / social_analysis['visits'] * 100
social_analysis['Тип'] = ['Другие источники', 'Соцсети']
print(social_analysis[['Тип', 'visits', 'conversions', 'CR']].to_string(index=False))

if data['is_social'].sum() > 0:
    social_cr = data[data['is_social']]['has_target_action'].mean()
    non_social_cr = data[~data['is_social']]['has_target_action'].mean()
    print(f"\nВывод:")
    print(f"  - Трафик из соцсетей: {social_cr*100:.4f}% CR ({data['is_social'].sum():,} визитов)")
    print(f"  - Другие источники: {non_social_cr*100:.4f}% CR ({(~data['is_social']).sum():,} визитов)")
    if social_cr > non_social_cr:
        print("  - Рекомендация: Да, трафик из соцсетей показывает более высокий CR")
    else:
        print("  - Рекомендация: Пока не рекомендуется увеличивать, CR ниже среднего")

# ============================================================================
# 5. SAVE RESULTS
# ============================================================================
print("\n" + "=" * 60)
print("СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")
print("=" * 60)

# Save summary statistics
summary = {
    'total_sessions': len(data),
    'total_conversions': data['has_target_action'].sum(),
    'overall_CR': CR * 100,
    'hypothesis_1_significant': result1['significant'],
    'hypothesis_2_significant': result2['significant'],
    'hypothesis_3_significant': result3['significant']
}

summary_df = pd.DataFrame([summary])
summary_df.to_csv(os.path.join(OUTPUT_PATH, 'summary_statistics.csv'), index=False)
print(f"Сохранено: summary_statistics.csv")

print("\n" + "=" * 60)
print("ЧАСТЬ 1 (DATA ANALYST) ЗАВЕРШЕНА")
print("=" * 60)
[README.md](https://github.com/user-attachments/files/26027142/README.md)
# СберАвтоподписка - Анализ данных сайта

[![Python](https://img.shields.io/badge/Python-3.8+-blue)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-1.5+-green)](https://pandas.pydata.org/)
[![Scikit-learn](https://img.shields.io/badge/Scikit--learn-1.0+-orange)](https://scikit-learn.org/)
[![Flask](https://img.shields.io/badge/Flask-2.0+-grey)](https://flask.palletsprojects.com/)

## Описание проекта

Финальная работа по курсу "Введение в Data Science". Анализ данных сайта «СберАвтоподписка» — сервиса долгосрочной аренды автомобилей для физических лиц.

Проект включает три специализации:
- **Data Analyst** — разведочный анализ, проверка гипотез, бизнес-вопросы
- **ML Engineer** — предиктивная модель, API сервис
- **Data Engineer** — база данных, пайплайн обработки данных

## Структура проекта

```
SberAutoSubscription/
├── da/                         # Data Analyst
│   ├── da_analysis.py          # Основной анализ с визуализациями
│   ├── da_notebook.ipynb       # Jupyter Notebook
│   ├── eda_visualizations.png  # Визуализации распределений
│   ├── eda_correlation_heatmap.png  # Heatmap корреляций
│   └── summary_statistics.csv # Итоговые статистики
│
├── ml/                         # ML Engineer
│   ├── ml_model_fast.py       # Обучение модели
│   ├── ml_api.py              # API сервис (Flask)
│   ├── model.pkl              # Обученная модель (Random Forest)
│   └── ml_notebook.ipynb     # Jupyter Notebook
│
├── de/                        # Data Engineer
│   ├── de_setup.py           # Создание SQLite БД
│   ├── de_run_pipeline.py   # Пайпайн для новых JSON
│   ├── new_data/            # Папка для новых данных
│   │   └── example.json     # Пример JSON файла
│   └── de_notebook.ipynb   # Jupyter Notebook
│
├── docs/                     # Документация
│   └── README.md            # Этот файл
│
└── data/                     # Исходные данные (не в репозитории)
    ├── ga_sessions.csv      # Сессии (1.86M строк)
    └── ga_hits.csv          # Хиты (15.7M строк)
```

## Результаты

### Data Analyst
| Метрика | Значение |
|---------|----------|
| Всего сессий | 1,860,042 |
| Конверсия (CR) | 2.70% |
| Гипотезы проверены | 3 (все отклонены) |

**Гипотезы:**
- Органический vs Платный трафик: CR 4.04% vs 2.19% (p < 0.001)
- Мобильный vs Десктоп: CR 2.60% vs 3.14% (p < 0.001)
- Города присутствия vs Другие: CR 2.79% vs 2.58% (p < 0.001)

**Бизнес-вопросы:**
- Лучший трафик: desktop + органический
- Топ города: Москва, Санкт-Петербург
- Соцсети: не рекомендуется увеличивать (низкий CR)

### ML Engineer
| Метрика | Значение |
|---------|----------|
| Модель | Random Forest |
| ROC-AUC | 0.6629 |
| Топ фичи | utm_source, utm_medium, is_organic |

### Data Engineer
| Компонент | Описание |
|-----------|----------|
| База данных | SQLite (sberautopodpiska.db) |
| Таблицы | ga_sessions, ga_hits |
| Индексы | session_id, visit_date, utm_source |
| Пайпайн | Обработка новых .json файлов |

## Установка и запуск

### Требования
```bash
pip install pandas numpy matplotlib seaborn scikit-learn scipy flask
```

### Data Analyst
```bash
cd SberAutoSubscription/da
python da_analysis.py
```
Результаты: графики в `*.png` файлах, статистика в консоли.

### ML Engineer
```bash
# Обучение модели
cd SberAutoSubscription/ml
python ml_model_fast.py

# Запуск API
python ml_api.py
# POST http://localhost:5000/predict
```

Пример запроса:
```json
{
  "visit_number": 1,
  "visit_hour": 14,
  "visit_dayofweek": 2,
  "device_category": "desktop",
  "utm_medium": "cpc",
  "utm_source": "ZpYIoDJMcFzVoPFsHGJL",
  "geo_country": "Russia",
  "geo_city": "Moscow"
}
```

### Data Engineer
```bash
# Создание БД
cd SberAutoSubscription/de
python de_setup.py

# Запуск пайплайна (положите .json в new_data/)
python de_run_pipeline.py
```

## Глоссарий

| Термин | Описание |
|--------|----------|
| CR (Conversion Rate) | Конверсия из визита в целевое действие |
| Целевое действие | Оставить заявку, заказать звонок и т.д. |
| Органический трафик | utm_medium in ('organic', 'referral', '(none)') |
| Трафик из соцсетей | Определённые utm_source (см. код) |

## Технологии

- **Python 3.8+** — основной язык
- **Pandas** — работа с данными
- **Scikit-learn** — ML модели
- **Matplotlib/Seaborn** — визуализации
- **Scipy** — статистические тесты
- **Flask** — API сервис
- **SQLite** — база данных

## Лицензия

Учебный проект. Данные предоставлены партнёром.

## Автор

Data Science Student

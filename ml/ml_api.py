"""
СберАвтоподписка - ML API сервис
Запуск: python ml_api.py
Использование: POST http://localhost:5000/predict с JSON данными
"""

from flask import Flask, request, jsonify
import pickle
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import os

app = Flask(__name__)

# Load model
MODEL_PATH = os.path.join(os.path.dirname(__file__), 'model.pkl')

print("Загрузка модели...")
try:
    with open(MODEL_PATH, 'rb') as f:
        model_data = pickle.load(f)
    model = model_data['model']
    feature_cols = model_data['features']
    print(f"Модель загружена. ROC-AUC: {model_data.get('auc', 'N/A')}")
except Exception as e:
    print(f"Ошибка загрузки модели: {e}")
    model = None
    feature_cols = None

# Known categories for encoding (from training)
KNOWN_CATEGORIES = {
    'device_category': ['mobile', 'desktop', 'tablet'],
    'utm_medium': ['banner', 'cpc', '(none)', 'cpm', 'referral', 'organic', 'email', 'push'],
    'utm_source': ['ZpYIoDJMcFzVoPFsHGJL', 'fDLlAcSmythWSCVMvqvL', 'kjsLglQLzykiRbcDiGcD'],
    'geo_country': ['Russia', 'unknown'],
    'geo_city': ['Moscow', 'Saint Petersburg', 'unknown']
}

def encode_category(value, category_type):
    """Encode categorical value"""
    known = KNOWN_CATEGORIES.get(category_type, ['unknown'])
    if value is None or pd.isna(value):
        return len(known)
    val = str(value).lower()
    for i, k in enumerate(known):
        if k.lower() in val or val in k.lower():
            return i
    return len(known)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok' if model is not None else 'error',
        'model_loaded': model is not None
    })

@app.route('/predict', methods=['POST'])
def predict():
    """Predict endpoint"""
    if model is None:
        return jsonify({'error': 'Model not loaded'}), 500
    
    try:
        data = request.get_json()
        
        # Extract features
        features = {}
        
        # Numeric features
        features['visit_number'] = int(data.get('visit_number', 1))
        features['visit_hour'] = int(data.get('visit_hour', 12))
        features['visit_dayofweek'] = int(data.get('visit_dayofweek', 0))
        
        # Binary features
        features['is_organic'] = 1 if data.get('utm_medium') in ['organic', 'referral', '(none)'] else 0
        features['is_mobile'] = 1 if data.get('device_category') == 'mobile' else 0
        features['is_desktop'] = 1 if data.get('device_category') == 'desktop' else 0
        features['is_first_visit'] = 1 if features['visit_number'] == 1 else 0
        
        # Categorical features (encoded)
        features['device_category_enc'] = encode_category(data.get('device_category'), 'device_category')
        features['utm_medium_enc'] = encode_category(data.get('utm_medium'), 'utm_medium')
        features['utm_source_enc'] = encode_category(data.get('utm_source'), 'utm_source')
        features['geo_country_enc'] = encode_category(data.get('geo_country'), 'geo_country')
        features['geo_city_enc'] = encode_category(data.get('geo_city'), 'geo_city')
        
        # Create dataframe in correct order
        X = pd.DataFrame([features])[feature_cols].fillna(0)
        
        # Predict
        prob = model.predict_proba(X)[0][1]
        prediction = 1 if prob > 0.5 else 0
        
        return jsonify({
            'prediction': prediction,
            'probability': float(prob),
            'will_convert': bool(prediction)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/', methods=['GET'])
def index():
    return jsonify({
        'service': 'SberAutoSubscription ML Predictor',
        'endpoints': {
            '/health': 'Health check',
            '/predict': 'POST prediction (POST JSON with visit data)'
        },
        'example_input': {
            'visit_number': 1,
            'visit_hour': 14,
            'visit_dayofweek': 2,
            'device_category': 'desktop',
            'utm_medium': 'cpc',
            'utm_source': 'ZpYIoDJMcFzVoPFsHGJL',
            'geo_country': 'Russia',
            'geo_city': 'Moscow'
        }
    })

if __name__ == '__main__':
    print("\n" + "="*50)
    print("ML API сервис СберАвтоподписка")
    print("="*50)
    print("Запущен на http://localhost:5000")
    print("Точки доступа:")
    print("  GET  /         - Информация")
    print("  GET  /health   - Проверка здоровья")
    print("  POST /predict  - Предсказание")
    print("="*50 + "\n")
    app.run(host='0.0.0.0', port=5000, debug=False)
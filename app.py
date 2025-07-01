from flask import Flask, render_template, request
import pandas as pd
import joblib

app = Flask(__name__)
model = joblib.load("artifacts/model.pkl")
train_df = pd.read_csv("artifacts/train.csv")
feature_medians = train_df.median()

def categorize_aqi(aqi):
    if aqi <= 50:
        return "Good", "green"
    elif aqi <= 100:
        return "Satisfactory", "yellowgreen"
    elif aqi <= 200:
        return "Moderate", "orange"
    elif aqi <= 300:
        return "Poor", "red"
    elif aqi <= 400:
        return "Very Poor", "purple"
    else:
        return "Severe", "maroon"

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    try:
        feature_names = ['PM2_5', 'PM10', 'NO', 'NO2', 'NOx', 'NH3', 
                         'CO', 'SO2', 'O3', 'Benzene', 'Toluene', 'Xylene']
        data = {}

        for feature in feature_names:
            val = request.form.get(feature)
            data[feature] = float(val) if val else None

        input_df = pd.DataFrame([data])
        
        input_df = input_df.fillna(feature_medians)

        prediction = model.predict(input_df)[0]

        category, color = categorize_aqi(prediction)
        return render_template(
            'index.html',
            prediction_text=f"Predicted AQI: {round(prediction, 2)} ({category})",
            prediction_color=color
        )

    except Exception as e:
        return render_template('index.html', prediction_text="Error: " + {str(e)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
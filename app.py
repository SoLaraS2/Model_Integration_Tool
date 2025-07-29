from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import requests
from io import StringIO
import tempfile
import os

app = Flask(__name__)
CORS(app)

CSV_BASE_URL = "https://your-sharepoint-link.com/path/"  # Replace with your base SharePoint path

@app.route("/process", methods=["POST"])
def process_data():
    data = request.json
    year = data.get("year")
    scenario = data.get("scenario")
    weather_year = data.get("weather_year")
    custom_values = data.get("custom_values", {})  # {(state, subsector): percentage}
    fallback_scenarios = data.get("fallback_scenarios", {})  # {subsector: scenario or 'baseline'}

    filename = f"{year}_{scenario}.csv"
    url = CSV_BASE_URL + filename

    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.content.decode("utf-8")))

        df["weather_year"] = pd.to_datetime(df["weather_datetime"]).dt.year
        df = df[df["weather_year"] == int(weather_year)]

        # Apply custom percentages
        for (state, subsector), percent in custom_values.items():
            mask = (df["subsector"] == subsector)
            if state in df.columns:
                df.loc[mask, state] *= float(percent)

        # Load fallback scenarios per subsector if needed
        for subsector, fallback_scenario in fallback_scenarios.items():
            if fallback_scenario == scenario:
                continue
            fb_filename = f"{year}_{fallback_scenario}.csv"
            fb_url = CSV_BASE_URL + fb_filename
            fb_df = pd.read_csv(StringIO(requests.get(fb_url).content.decode("utf-8")))
            fb_df = fb_df[pd.to_datetime(fb_df["weather_datetime"]).dt.year == int(weather_year)]
            for state in df.columns[4:]:
                df.loc[df["subsector"] == subsector, state] = fb_df.loc[fb_df["subsector"] == subsector, state].values

        # Save and return downloadable CSV
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp.name, index=False)
        tmp.close()
        return send_file(tmp.name, as_attachment=True, download_name="custom_output.csv")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)

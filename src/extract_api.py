import requests
import pandas as pd


def get_indicator(indicator_code, column_name):

    url = f"https://api.worldbank.org/v2/country/COL/indicator/{indicator_code}?format=json"

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API request failed: {response.status_code}")

    try:
        data = response.json()
    except Exception:
        raise Exception("API did not return valid JSON")

    if not data or len(data) < 2:
        raise Exception("API returned empty dataset")

    records = data[1]

    df = pd.DataFrame(records)[["date", "value"]]

    df.columns = ["year", column_name]

    df["year"] = df["year"].astype(int)

    return df


def extract_api():

    print("Extracting API data from World Bank...")

    poverty = get_indicator(
        "SI.POV.DDAY",
        "poverty_rate"
    )

    unemployment = get_indicator(
        "SL.UEM.TOTL.ZS",
        "unemployment_rate"
    )

    gdp = get_indicator(
        "NY.GDP.PCAP.CD",
        "gdp_per_capita"
    )

    gini = get_indicator(
        "SI.POV.GINI",
        "gini_index"
    )

    df_api = poverty.merge(unemployment, on="year", how="left")
    df_api = df_api.merge(gdp, on="year", how="left")
    df_api = df_api.merge(gini, on="year", how="left")

    print("API extraction finished")

    return df_api
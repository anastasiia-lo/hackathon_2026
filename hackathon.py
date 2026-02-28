import os
import sqlite3
import pandas as pd
import geopandas as gpd
from flask import Flask, render_template, request, redirect, url_for
from scipy.spatial import cKDTree

app = Flask(__name__)

states = gpd.read_file("us-states.json")
ny_boundary = states[states['name'] == 'New York']


def process_coordinates(conn, table_name="ny_customers"):

    df_orders = pd.read_sql(f"SELECT * FROM {table_name}", con=conn)
    df_ny = pd.read_sql("SELECT * FROM TAX_NY", con=conn)

    df_orders = df_orders.dropna(subset=['latitude', 'longitude'])
    df_ny_clean = df_ny.dropna(subset=['LAT', 'LNG'])

    tree = cKDTree(df_ny_clean[['LNG', 'LAT']].values)
    d, i = tree.query(df_orders[['longitude', 'latitude']].values, k=1)

    df_orders['ZipCode'] = df_ny_clean.iloc[i]['ZipCode'].values
    df_ny_tax_data = df_ny.drop_duplicates(subset=['ZipCode'])

    df_final = pd.merge(df_orders, df_ny_tax_data, on='ZipCode', how='left', suffixes=('', '_db'))

    cols_to_drop = ['latitude', 'longitude', 'LAT', 'LNG', 'ZipCode', 'distance', 'geometry']
    return df_final.drop(columns=cols_to_drop, errors='ignore')

def process_tax_calculation(df_input, output_filename="result.csv"):

    df_finance = df_input.copy()
    if 'id' not in df_finance.columns:
        df_finance.insert(0, 'id', range(1, len(df_finance) + 1))

    df_finance.columns = df_finance.columns.str.strip()
    col_sum = 'subtotal'

    tax_rates = ['EstimatedCombinedRate', 'StateRate', 'EstimatedCountyRate',
                 'EstimatedCityRate', 'EstimatedSpecialRate']

    for col in tax_rates + [col_sum]:
        if col in df_finance.columns:
            df_finance[col] = pd.to_numeric(df_finance[col], errors='coerce').fillna(0)

    df_finance['state_tax_amount'] = (df_finance[col_sum] * df_finance['StateRate']).round(2)
    df_finance['county_tax_amount'] = (df_finance[col_sum] * df_finance['EstimatedCountyRate']).round(2)
    df_finance['city_tax_amount'] = (df_finance[col_sum] * df_finance['EstimatedCityRate']).round(2)
    df_finance['especial_tax_amount'] = (df_finance[col_sum] * df_finance['EstimatedSpecialRate']).round(2)
    df_finance['tax_amount'] = (df_finance[col_sum] * df_finance['EstimatedCombinedRate']).round(2)
    df_finance['total_amount'] = (df_finance[col_sum] + df_finance['tax_amount']).round(2)

    first_cols = ['id', 'ReportingCode', 'State', 'TaxRegionName', 'subtotal', 'StateRate',
                  'state_tax_amount', 'EstimatedCountyRate', 'county_tax_amount',
                  'EstimatedCityRate', 'city_tax_amount', 'EstimatedSpecialRate',
                  'especial_tax_amount', 'EstimatedCombinedRate', 'tax_amount', 'total_amount']

    existing_first = [c for c in first_cols if c in df_finance.columns]
    other_cols = [c for c in df_finance.columns if c not in existing_first]
    df_finance = df_finance[existing_first + other_cols]

    df_finance.to_csv(output_filename, index=False)
    return df_finance

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():

    if 'file' not in request.files: return "Файл не вибрано"

    file = request.files['file']

    if file and file.filename.endswith('.csv'):

        try:
            df = pd.read_csv(file)
            gdf_points = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326")
            filtered_points = gpd.sjoin(gdf_points, ny_boundary, predicate='within')

            conn = sqlite3.connect('customers.db')
            pd.DataFrame(filtered_points).drop(columns='geometry', errors='ignore').to_sql('ny_customers', conn, if_exists='replace', index=False)

            df_ready = process_coordinates(conn, "ny_customers")
            process_tax_calculation(df_ready, "result.csv")

            conn.close()

            return redirect(url_for('display_results'))

        except Exception as e:

            return f"Помилка завантаження: {e}"

    return "Тільки .csv"

@app.route('/submit_form', methods=['POST'])
def handle_form():

    try:

        lng = float(request.form.get('longitude'))
        lat = float(request.form.get('width'))
        subtotal = float(request.form.get('sum'))

        df_row = pd.DataFrame({'longitude': [lng], 'latitude': [lat], 'subtotal': [subtotal]})
        gdf = gpd.GeoDataFrame(df_row, geometry=gpd.points_from_xy(df_row.longitude, df_row.latitude), crs="EPSG:4326")

        if gpd.sjoin(gdf, ny_boundary, predicate='within').empty:
            return "Координати поза межами NY!"

        conn = sqlite3.connect('customers.db')
        df_row.to_sql('ny_customers_temp', conn, if_exists='replace', index=False)

        df_ready = process_coordinates(conn, "ny_customers_temp")
        final_row_df = process_tax_calculation(df_ready, "temp_result.csv")
        conn.close()

        history_file = "history_form.csv"
        file_exists = os.path.isfile(history_file)
        final_row_df.to_csv(history_file, mode='a', index=False, header=not file_exists)

        return redirect(url_for('display_results'))
    except Exception as e:
        return f"Помилка форми: {e}"

@app.route('/results', methods=['GET'])
def display_results():

    table_main = None

    if os.path.exists("result.csv"):

        df = pd.read_csv("result.csv")
        table_main = df.to_html(classes='display nowrap', table_id='resultTable', index=False)

    table_history = None

    if os.path.exists("history_form.csv"):

        df_h = pd.read_csv("history_form.csv")
        table_history = df_h.to_html(classes='display nowrap', table_id='historyTable', index=False)

    return render_template('index.html', table=table_main, history_table=table_history)

if __name__ == '__main__':
    app.run(debug=True)

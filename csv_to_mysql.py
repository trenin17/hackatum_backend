import mysql.connector
import csv
from datetime import datetime

# Функция для чтения данных из CSV
def read_csv(file_path):
    data = []
    with open(file_path, mode='r', encoding='utf-8-sig') as csvfile:
        csvreader = csv.DictReader(csvfile, delimiter=';')
        for row in csvreader:
            data.append(row)
    return data


cnx = mysql.connector.connect(user='Python', password='Python1!',
                                host='localhost',
                                database="electricity")

cursor = cnx.cursor()
add_to_table_consumption = ("INSERT INTO consumption"
                            "(consumption, residual_load, hydro_pumped_storage, production, date, timestamp) "
                            "VALUES (%s, %s, %s, %s, %s, FROM_UNIXTIME(%s))")


def export_to_mysql(data_consumption, data_production):
    for consumption, production in zip(data_consumption, data_production):
        # Преобразование строки даты и времени в timestamp
        date_time_str = f"{consumption['Date']} {consumption['Start']}"
        date_time_obj = datetime.strptime(date_time_str, '%b %d, %Y %I:%M %p')
        timestamp = date_time_obj.timestamp()
        #timestamp = calculate_timestamp(consumption['Start'], consumption['End'], consumption['Date'])

        # Получение значения потребления электроэнергии
        #print(consumption['Total (grid load) [MWh] Original resolutions'].replace(',', ''))
        grid_load_value = float(consumption['Total (grid load) [MWh] Original resolutions'].replace(',', ''))
        residual_load_value = float(consumption['Residual load [MWh] Original resolutions'].replace(',', ''))
        hydro_pumped_storage = float(consumption['Hydro pumped storage [MWh] Original resolutions'].replace(',', ''))
        date = consumption['Date']

        production_total = (float(production['Biomass [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Hydropower [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Wind offshore [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Wind onshore [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Photovoltaics [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Other renewable [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Nuclear [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Lignite [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Hard coal [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Fossil gas [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Hydro pumped storage [MWh] Originalauflösungen'].replace(',', '')) +
                float(production['Other conventional [MWh] Originalauflösungen'].replace(',', ''))
                )

        cursor.execute(add_to_table_consumption, (grid_load_value, residual_load_value, hydro_pumped_storage, production_total, date, timestamp))

# Основной цикл
def main():
    # Путь к файлу CSV
    csv_file_path_consumption = 'Actual_consumption_202301010000_202311102359_Quarterhour.csv'
    csv_file_path_production = 'Actual_generation_202301010000_202311102359_Viertelstunde.csv'

    data_consumption = read_csv(csv_file_path_consumption)
    data_production = read_csv(csv_file_path_production)
    export_to_mysql(data_consumption, data_production)
    cnx.commit()
    cursor.close()
    cnx.close()

if __name__ == "__main__":
    main()


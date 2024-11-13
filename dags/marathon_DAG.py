from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
from datetime import datetime, timedelta, time
import pendulum

local_tz = pendulum.timezone("UTC") 
start_date = pendulum.now(tz=local_tz)

with DAG(
    dag_id="marathon_time_dag",
    start_date=start_date,
    schedule_interval=timedelta(days=7),
    max_active_runs=1,
    render_template_as_native_obj=True
) as dag:
    def extract_data_callable():
        # Print message, return a response
        print("Extracting data from workout API")
        url = 'http://django-backend:8000/api/workouts/weekly/'
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            raise ValueError(f"API response error: {response.status_code}")

    extract_data = PythonOperator(
        dag=dag,
        task_id="extract_data",
        python_callable=extract_data_callable
    )

    def encode_gender(data):
        if data == 'M':
            return 1
        elif data == 'F':
            return 0
        else:
            return


    def transform_data_callable(raw_data):
        df = pd.DataFrame(raw_data)

        grouped_df = df.groupby('user_id').agg({
            'distance': 'mean',
            'heartrate': 'mean',
            'age': 'first',
            'gender': 'first',
            'weight': 'first',
        }).reset_index()

        grouped_df['gender'] = grouped_df['gender'].apply(encode_gender)

        return grouped_df.to_dict(orient='records')


    transform_data = PythonOperator(
        dag=dag,
        task_id="transform_data",
        python_callable=transform_data_callable,
        op_kwargs={"raw_data": "{{ ti.xcom_pull(task_ids='extract_data') }}"}
    )

    def predict(data):
        print(data)
        try:
            response = requests.post('http://fastapi-backend:8001/predict/marathon_time_bulk', json=data)
            response.raise_for_status()
            return response.json().get('predictions')
        except requests.RequestException as e:
            raise ValueError(f"Error al llamar a la API: {e}")

    def predict_data_callable(transformed_data):
        loaded_data = pd.DataFrame(transformed_data)

        data_to_predict = [
            {'edad': row['age'],
             'peso': row['weight'], 
             'genero': row['gender'],
             'bpm': row['heartrate'],
             'distancia': row['distance']
            }
            for _, row in loaded_data.iterrows()
        ]

        predictions = predict(data_to_predict)

        # Agregar las predicciones al DataFrame
        loaded_data['time_prediction'] = predictions

        rows_to_keep = ['user_id', 'time_prediction']
        loaded_data = loaded_data[rows_to_keep]

        # Retornar los datos como una lista de diccionarios para tareas posteriores
        return loaded_data.to_dict(orient='records')
    
    predict_data = PythonOperator(
        dag=dag,
        task_id="predict_data",
        python_callable=predict_data_callable,
        op_kwargs={"transformed_data": "{{ ti.xcom_pull(task_ids='transform_data') }}"}
    )

    def bulk_update(data):
        try:
            response = requests.put('http://django-backend:8000/api/users/bulk-update-marathon-time/', json=data)
            response.raise_for_status()
        except requests.RequestException as e:
            raise ValueError(f"Error al llamar a la API: {e}")
        
    def format_time(value):
        try:
            # Convertir minutos a horas, minutos y segundos
            hours, remainder = divmod(value, 60)
            minutes, seconds = divmod(remainder * 60, 60)

            # Crear un objeto time
            formatted_time = time(int(hours), int(minutes), int(seconds))
            return formatted_time.isoformat()  # Convertir a formato ISO
        except Exception as e:
            raise ValueError(f"Error formateando el tiempo: {e}")

    def load_data_callable(predicted_data):
        df = pd.DataFrame(predicted_data)
        print(df)

        row_names = {
            'user_id': 'id',
            'time_prediction': 'marathon_time'
        }

        df = df.rename(columns=row_names)
        
        df['marathon_time'] = df['marathon_time'].apply(format_time)

        data = df.to_dict(orient='records')

        bulk_update(data)


    load_data = PythonOperator(
        dag=dag,
        task_id="load_data",
        python_callable=load_data_callable,
        op_kwargs={"predicted_data": "{{ ti.xcom_pull(task_ids='predict_data') }}"}
    )

# Set dependencies between tasks
# extract_data >> transform_data >> load_data
extract_data >> transform_data >> predict_data >> load_data

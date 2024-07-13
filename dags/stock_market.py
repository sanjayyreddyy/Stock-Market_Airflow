from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime   
import json, requests
from airflow.operators.python import PythonOperator
from include.stock_market.tasks import _get_stock_prices


symbol = "AAPL"

@dag(
    start_date=datetime(2024,1,1),
    schedule='@daily',
    catchup=False,
    tags=['Stock_Market']
)

def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available()->PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url,headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done = condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id = "get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={'url' : '{{task_instance.xcom_pull(task_ids="is_api_available")}}', 'symbol': symbol}
    )

    is_api_available() >> get_stock_prices

stock_market()
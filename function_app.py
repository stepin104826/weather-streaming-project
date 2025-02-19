import logging
import azure.functions as func
import requests
import json
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

app = func.FunctionApp()

@app.timer_trigger(schedule="*/30 * * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def weather_api_function(myTimer: func.TimerRequest):
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')

    # Event Hub Configuration
    EVENT_HUB_NAME = "weather-streaming-eventhub"
    EVENT_HUB_NAMESPACE = "weather-streaming-namespace27.servicebus.windows.net"

    # Uses managed identity of function app
    credential = DefaultAzureCredential()

    # Initialize the Event Hub Producer
    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        credential=credential
    )

    # Function to create and send events
    def send_event(event):
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(event)))
        producer.send_batch(event_data_batch)

    # Function to handle the API response
    def handle_response(response):
        if response.status_code == 200:
            return response.json()
        else:
            return f"Error: {response.status_code}, {response.text}"

    # Function to get current weather and air quality status
    def get_current_weather(base_url, api_key, location):
        current_weather_url = f"{base_url}/current.json"
        params = {
            'key': api_key,
            'q': location,
            'aqi': 'yes'
        }
        response = requests.get(url=current_weather_url, params=params)
        return handle_response(response)

    # Function to forecast data
    def get_forecast_data(base_url, api_key, days, location):
        forecast_weather_url = f"{base_url}/forecast.json"
        params = {
            'key': api_key,
            'days': days,
            'q': location
        }
        response = requests.get(url=forecast_weather_url, params=params)
        return handle_response(response)

    # Function to get alerts
    def get_alerts(base_url, api_key, location):
        alerts_url = f"{base_url}/alerts.json"
        params = {
            'key': api_key,
            'q': location,
            'alerts': 'yes'
        }
        response = requests.get(url=alerts_url, params=params)
        return handle_response(response)

    # Flatten and merge the data
    def flatten_data(current_weather, forecast_weather, alerts):
        location_data = current_weather.get("location", {})
        current = current_weather.get("current", {})
        condition = current.get("condition", {})
        air_quality = current.get("air_quality", {})
        forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
        alert_list = alerts.get("alerts", {}).get("alert", [])

        flattened_data = {
            'name': location_data.get('name'),
            'region': location_data.get('region'),
            'country': location_data.get('country'),
            'lat': location_data.get('lat'),
            'lon': location_data.get('lon'),
            'localtime': location_data.get('localtime'),
            'temp_c': current.get('temp_c'),
            'is_day': current.get('is_day'),
            'condition_text': condition.get('text'),
            'condition_icon': condition.get('icon'),
            'wind_kph': current.get('wind_kph'),
            'wind_dir': current.get('wind_dir'),
            'wind_degree': current.get('wind_degree'),
            'pressure_in': current.get('pressure_in'),
            'precip_in': current.get('precip_in'),
            'humidity': current.get('humidity'),
            'cloud': current.get('cloud'),
            'feelslike_c': current.get('feelslike_c'),
            'uv': current.get('uv'),
            'air_quality': {
                'co': air_quality.get('co'),
                'no2': air_quality.get('no2'),
                'o3': air_quality.get('o3'),
                'so2': air_quality.get('so2'),
                'pm2_5': air_quality.get('pm2_5'),
                'pm10': air_quality.get('pm10'),
                'us-epa-index': air_quality.get('us-epa-index'),
                'gb-defra-index': air_quality.get('gb-defra-index')
            },
            'alerts': [
                {
                    'headline': alert.get('headline'),
                    'severity': alert.get('severity'),
                    'description': alert.get('description'),
                    'instruction': alert.get('instruction')
                }
                for alert in alert_list
            ],
            'forecast': [
                {
                    'date': day.get('date'),
                    'maxtemp_c': day.get('day', {}).get('maxtemp_c'),
                    'mintemp_c': day.get('day', {}).get('mintemp_c'),
                    'condition': day.get('day', {}).get('condition', {}).get('text')
                }
                for day in forecast
            ]
        }

        return flattened_data

    # Get the weather API key from Key Vault using Function App managed identity
    def get_secret_from_keyvault(kv_uri, secret_name):
        secret_client = SecretClient(vault_url=kv_uri, credential=credential)
        retrieved_secret = secret_client.get_secret(secret_name)
        return retrieved_secret.value

    # Main program
    def fetch_weather_data():
        base_url = "http://api.weatherapi.com/v1"
        location = "Buffalo"

        # Fetch the API weather key from Key Vault
        VaultURI = "https://kv-weather-streaming27.vault.azure.net/"
        API_KEY_SECRET_NAME = "weather-api-key"
        weather_api_key = get_secret_from_keyvault(VaultURI, API_KEY_SECRET_NAME)

        # Get data from API
        current_weather = get_current_weather(base_url, weather_api_key, location)
        forecast_weather = get_forecast_data(base_url, weather_api_key, 5, location)
        alerts = get_alerts(base_url, weather_api_key, location)

        # Flatten and merge data
        merged_data = flatten_data(current_weather, forecast_weather, alerts)
        print("Weather data: ", json.dumps(merged_data, indent=3))
        send_event(merged_data)  

    fetch_weather_data()
    producer.close()

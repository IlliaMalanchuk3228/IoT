from paho.mqtt import client as mqtt_client
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from file_datasource import FileDatasource
import config

def connect_mqtt(broker, port):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print(f"Failed to connect {broker}:{port}, return code {rc}")
            exit(rc)
    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client

def publish(client, topic, datasource, delay):
    while True:
        time.sleep(delay)
        try:
            data = datasource.read()
            msg = AggregatedDataSchema().dumps(data)
            result = client.publish(topic, msg)
            if result[0] == 0:
                pass  # Successfully sent
            else:
                print(f"Failed to send message to topic {topic}")
        except ValueError as e:
            print(e)
            break  # Or handle it differently

def run():
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    datasource.startReading()
    datasource.startReadingParking()
    publish(client, config.MQTT_TOPIC, datasource, config.DELAY)

if __name__ == '__main__':
    run()

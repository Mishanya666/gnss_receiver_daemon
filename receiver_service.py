import os
import sys
import datetime
import paho.mqtt.client as mqtt_client
import time
import threading
import random

broker = "broker.emqx.io"
topic_prefix = "gnss/data/"

processed_messages = set()
subscription_event = threading.Event()

def on_message(client, userdata, message):
    data = str(message.payload.decode("utf-8"))
    if data in processed_messages:
        return
    try:
        parts = data.split(' ', 2)
        if len(parts) < 3:
            raise ValueError("Invalid message format")

        original_date_str, original_time_str, rest_data = parts
        original_time_str = f"{original_date_str} {original_time_str}"

        original_time = datetime.datetime.strptime(original_time_str, "%Y-%m-%d %H:%M:%S")
        current_time = datetime.datetime.utcnow()

        if abs((current_time - original_time).total_seconds()) <= 30:
            processed_messages.add(data)
            print(f"Processed message: {data}")
    except ValueError as e:
        print(f"Error parsing message: {e} - {data}")
    except Exception as e:
        print(f"Unexpected error: {e} - {data}")

client = mqtt_client.Client('user')
client.on_message = on_message

def subscribe_to_topic(receiver_name):
    try:
        client.connect(broker)
        client.loop_start()
        client.subscribe(f"{topic_prefix}{receiver_name}")
        print(f"Subscribed to topic: {topic_prefix}{receiver_name}")
        subscription_event.set()
    except Exception as ex:
        print(f"Error subscribing to topic: {ex}")
        client.disconnect()

def publish_simulated_data(receiver_name):
    subscription_event.wait()
    current_time = datetime.datetime.utcnow()
    start_time = current_time.replace(second=0, microsecond=0)

    while True:
        message_time = start_time + datetime.timedelta(seconds=30)
        if datetime.datetime.utcnow() >= message_time:
            gnss_list = ["G01", "G02", "G03", "G04", "G05", "G06", "G07", "G08", "G09", "G10",
                         "G11", "G12", "G13", "G14", "G15", "G16", "G17", "G18", "G19", "G20"]
            selected_gnss = random.choice(gnss_list)
            phase_tec = random.uniform(-500.0, 500.0)
            p_range_tec = random.uniform(-50.0, 50.0)

            message = f"{message_time.strftime('%Y-%m-%d %H:%M:%S')} {selected_gnss}: {phase_tec} {p_range_tec}"
            if message not in processed_messages:
                try:
                    client.publish(f"{topic_prefix}{receiver_name}", message)
                    print(f"Published message: {message}")
                    processed_messages.add(message)
                except Exception as ex:
                    print(f"Failed to send message to topic {topic_prefix}{receiver_name}: {ex}")
            start_time = message_time
        time.sleep(1)

def thread_exception_handler(args):
    if issubclass(args.exc_type, Exception):
        print(f"Exception in thread {args.thread.name}: {args.exc_type.__name__} {args.exc_value}")

if __name__ == "__main__":
    try:
        receiver_name = input("Enter receiver name to subscribe (or 'exit' to quit): ")
        if receiver_name.lower() != 'exit':
            threading.excepthook = thread_exception_handler
            subscribe_to_topic(receiver_name)
            publisher_thread = threading.Thread(target=publish_simulated_data, args=(receiver_name,))
            publisher_thread.start()
            client.loop_forever()
        else:
            sys.exit(0)
    except KeyboardInterrupt:
        print("Interrupted by user")
        client.disconnect()
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
        client.disconnect()

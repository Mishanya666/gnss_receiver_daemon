import os
import time
import paho.mqtt.client as mqtt_client
from gnss_tec import rnx
import glob
import sys
from daemonize import Daemonize

broker = "broker.emqx.io"
topic_prefix = "gnss/data/"
data_dir = "/home/kirill/Practice/rnx_files"

class GNSSReceiverDaemon:
    def __init__(self, receiver_name):
        self.receiver_name = receiver_name
        self.current_file_path = None
        self.client = mqtt_client.Client(client_id=receiver_name, protocol=mqtt_client.MQTTv5)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.connect(broker)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker with result code {rc}")
        self.start_processing()

    def on_disconnect(self, client, userdata, rc):
        print(f"Disconnected from MQTT broker with result code {rc}")

    def start_processing(self):
        while True:
            if not self.current_file_path or not os.path.exists(self.current_file_path):
                self.find_and_process_new_file()
            time.sleep(60)

    def find_and_process_new_file(self):
        pattern = os.path.join(data_dir, self.receiver_name, f"{self.receiver_name}_R_*.rnx")
        matching_files = glob.glob(pattern)
        if matching_files:
            new_file_path = matching_files[0]
            if new_file_path != self.current_file_path:
                self.current_file_path = new_file_path
                self.process_file(self.current_file_path)
        else:
            print(f"No matching files found for pattern: {pattern}")

    def process_file(self, file_path):
        try:
            with open(file_path) as obs_file:
                reader = rnx(obs_file)
                for tec in reader:
                    message = '{} {}: {} {}'.format(
                        tec.timestamp, tec.satellite, tec.phase_tec, tec.p_range_tec
                    )
                    self.client.publish(f"{topic_prefix}{self.receiver_name}", message)
                    print(f"Published message: {message}")
                    time.sleep(30)
        except FileNotFoundError as e:
            print(f"Error opening file: {e}")
        except Exception as e:
            print(f"Unexpected error processing file: {e}")

    def run(self):
        daemon = Daemonize(app=self.receiver_name, pid=f"/tmp/{self.receiver_name}.pid", action=self.start_processing, keep_fds=[])
        daemon.start()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python receiver_service.py <receiver_name>")
        sys.exit(1)

    receiver_name = sys.argv[1]
    daemon = GNSSReceiverDaemon(receiver_name)
    daemon.run()

import time
import paho.mqtt.client as mqtt_client
from gnss_tec import rnx
import sys
import os
import glob
import logging
from daemonize import Daemonize

broker = "broker.emqx.io"
topic_prefix = "gnss/data/"
log_dir = "logs"
data_dir = "/home/kirill/Practice/rnx_files"

if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, "receiver_service.log")
logging.basicConfig(filename=log_file, level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger("ReceiverService")

class GNSSReceiverDaemon:
    def __init__(self, receiver_name):
        self.receiver_name = receiver_name
        self.current_file_path = None
        self.client = mqtt_client.Client(client_id=receiver_name, protocol=mqtt_client.MQTTv5)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.connect(broker)
        self.client.loop_start()
        self.error_log_file = os.path.join(log_dir, f"{receiver_name}_error.log")

    def on_connect(self, client, userdata, flags, rc):
        logger.info(f"Connected to MQTT broker with result code {rc}")
        self.start_processing()

    def on_disconnect(self, client, userdata, rc):
        logger.info(f"Disconnected from MQTT broker with result code {rc}")

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
                logger.info(f"No new files found for {self.receiver_name}")
        else:
            logger.warning(f"No matching files found for pattern: {pattern}")

    def process_file(self, file_path):
        try:
            with open(file_path) as obs_file:
                reader = rnx(obs_file)
                for tec in reader:
                    message = '{} {}: {} {}'.format(
                        tec.timestamp, tec.satellite, tec.phase_tec, tec.p_range_tec
                    )
                    self.client.publish(f"{topic_prefix}{self.receiver_name}", message)
                    logger.info(f"Published message: {message}")
                    time.sleep(30)
        except FileNotFoundError as e:
            with open(self.error_log_file, 'a') as log:
                log.write(f"Error opening file: {e}\n")
            logger.error(f"Error opening file: {e}")
        except Exception as e:
            with open(self.error_log_file, 'a') as log:
                log.write(f"Unexpected error processing file: {e}\n")
            logger.error(f"Unexpected error processing file: {e}")

    def run(self):
        daemon = Daemonize(app=self.receiver_name, pid=f"/tmp/{self.receiver_name}.pid", action=self.start_processing, keep_fds=[])
        daemon.start()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Usage: python receiver_service.py <receiver_name>")
        sys.exit(1)

    receiver_name = sys.argv[1]
    daemon = GNSSReceiverDaemon(receiver_name)
    daemon.run()

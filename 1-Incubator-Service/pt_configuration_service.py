

# Configure python path to load incubator modules
import sys
import os
import logging
import logging.config
import time

# Get the current working directory. Should be 1-Incubator-Service
current_dir = os.getcwd()

assert os.path.basename(current_dir) == '1-Incubator-Service', 'Current directory is not 1-Incubator-Service'

# Get the parent directory. Should be the root of the repository
parent_dir = os.path.dirname(current_dir)

# The root of the repo should contain the incubator_dt folder. Otherwise something went wrong in 0-Pre-requisites.
assert os.path.exists(os.path.join(parent_dir, 'incubator_dt')), 'incubator_dt folder not found in the repository root'

incubator_dt_software_dir = os.path.join(parent_dir, 'incubator_dt', 'software')

assert os.path.exists(incubator_dt_software_dir), 'incubator_dt software directory not found'

# Add the parent directory to sys.path
sys.path.append(incubator_dt_software_dir)

from incubator.communication.server.rabbitmq import Rabbitmq
from incubator.communication.shared.protocol import ROUTING_KEY_STATE, ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS

class PTReconfigurationService:

    def __init__(self, max_diff, new_temp_desired, rabbitmq_config, new_max_temp, new_min_temp, original_max_temp, original_min_temp, anomaly_samples_required=2):

        self._rabbitmq = Rabbitmq(**rabbitmq_config)

        self._max_diff = max_diff # Maximum tolerated difference.
        assert self._max_diff > 0, "max_diff must be greater than 0."
        self._new_temp_desired = new_temp_desired # New desired temperature to set in the incubator in case there's an anomaly
        self._anomaly_samples_required = anomaly_samples_required
        assert self._anomaly_samples_required > 0, "anomaly_samples_required must be greater than 0."

        self._l = logging.getLogger("PTReconfigurationService")

        self.avg = None
        self.temp = None
        self.lt = None
        self._consecutive_anomaly_samples = 0

        self._new_max_temp = new_max_temp
        self._new_min_temp = new_min_temp
        self._original_max_temp = original_max_temp
        self._original_min_temp = original_min_temp
        self._is_reconfigured = False

    def setup(self):
        self._rabbitmq.connect_to_server()

        # Subscribe to any message coming from the incubator physical twin, and any message from the moving average service.
        # We use the same queue for both subscriptions, to ensure messages are processed in order.
        local_queue = self._rabbitmq.subscribe(routing_key=ROUTING_KEY_STATE,
                                on_message_callback=self.process_state_sample)

        # Notice the routing key we're binding the queue to. This is the same routing key that the moving average service uses to publish its outputs. 
        self._rabbitmq.channel.queue_bind(
            exchange=self._rabbitmq.exchange_name,
            queue=local_queue,
            #routing_key="incubator.record.dtcourse.moving_temperature_average_service.temperature_moving_average"
            routing_key="incubator.record.dtcourse.temperature_prediction_service.average_temperature"
        )

        self._l.info(f"PTReconfigurationService setup complete.")

    def process_state_sample(self, ch, method, properties, body_json):
        # Log the values received.
        self._l.info(f"Received state sample: {body_json}")

        # Run the state machine documented above.
        nt = body_json["time"]
        self.lt = nt  # Store last timestamp

        # if "average_temperature" in body_json["fields"]:
        #     self.temp = body_json["fields"]["average_temperature"]

        # if "moving_average_temperature" in body_json["fields"]:
        #     self.avg = body_json["fields"]["moving_average_temperature"]

        # Must change to (prediction service):
        if body_json.get("measurement") == "temperature_prediction_service":
            self.avg = body_json["fields"]["average_temperature"]
        elif "average_temperature" in body_json["fields"]:
            self.temp = body_json["fields"]["average_temperature"]

        if self.avg is not None and self.temp is not None:
            self.check_if_reconfiguration_needed()
            self.avg = None  # Reset avg to wait for the next moving average message.
            self.temp = None  # Reset temp to wait for the next state message.

    def check_if_reconfiguration_needed(self):

        diff = abs(self.temp - self.avg)

        # Prepare a message to publish under a new topic representing this service's data stream. We reuse the same timestamp from the original message, to enable easier traceability between outputs and inputs.
        timestamp = self.lt

        # The format of this message will become later. This is just to allow a uniform way to send data to the database.
        record_message = {
            "measurement": "pt_configuration_service",
            "time": timestamp,
            "tags": {
                "source": "pt_configuration_service"
            },
            "fields": {
                "difference": diff,
            }
        }

        # Log message
        self._l.info(f"Sending message: {record_message}")
        # Publish the message to the RabbitMQ server, just to record this service's output.
        self._rabbitmq.send_message("incubator.record.dtcourse.pt_configuration_service.temperature_moving_average", record_message)

        if diff <= self._max_diff:
            self._consecutive_anomaly_samples = 0
            if self._is_reconfigured:
                self._rabbitmq.send_message(ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS, {
                    "max_temp": self._original_max_temp,
                    "min_temp": self._original_min_temp,
                })
                self._is_reconfigured = False
                self._l.info(
                    f"Anomaly cleared. Restored controller thresholds to max={self._original_max_temp}, min={self._original_min_temp}."
                )
            self._l.info(f"No reconfiguration needed. Temp: {self.temp} and Avg: {self.avg}. Difference: {diff}")
            return 

        self._consecutive_anomaly_samples += 1
        self._l.info(
            f"Anomalous sample {self._consecutive_anomaly_samples}/{self._anomaly_samples_required}. "
            f"Temp: {self.temp} Avg: {self.avg} Difference: {diff}"
        )

        if self._consecutive_anomaly_samples < self._anomaly_samples_required:
            return

        self._l.info(
            f"Reconfiguration needed after {self._consecutive_anomaly_samples} anomalous samples. "
            f"Temp: {self.temp} and Avg: {self.avg}. Difference: {diff}"
        )

        self._rabbitmq.send_message(ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS, {
            "max_temp": self._new_max_temp,
            "min_temp": self._new_min_temp,
        })
        self._is_reconfigured = True

        # Reset the counter after issuing a reconfiguration command.
        self._consecutive_anomaly_samples = 0

        return

    def start_serving(self):
        self._rabbitmq.start_consuming()

if __name__ == "__main__":
    # Get utility functions to config logging and load configuration
    from incubator.config.config import load_config
    from pyhocon import ConfigFactory

    # Configure logging level to info
    logging.basicConfig(level=logging.INFO)

    # Get path to the startup.conf file used in the incubator dt:
    startup_conf = os.path.join(os.path.dirname(os.getcwd()), 'incubator_dt', 'software','startup.conf')
    assert os.path.exists(startup_conf), 'startup.conf file not found'

    # The startup.conf comes from the incubator dt repository.
    config = ConfigFactory.parse_file(startup_conf)
    service = PTReconfigurationService(
        max_diff=1.0,
        new_temp_desired=25.0,
        rabbitmq_config=config["rabbitmq"],
        new_max_temp=26.0,
        new_min_temp=24.0,
        original_max_temp=39.0,
        original_min_temp=36.0,
        anomaly_samples_required=2,
    )

    service.setup()

    # Start the PTReconfigurationService
    service.start_serving()

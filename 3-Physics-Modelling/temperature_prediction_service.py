
# Configure python path to load incubator modules
import sys
import os
import logging
import logging.config
import time

from temperature_prediction_model import IncubatorPredictionNN 

# Get the current working directory. Should be 3-Physics-Modelling
current_dir = os.getcwd()

assert os.path.basename(current_dir) == '3-Physics-Modelling', 'Current directory is not 3-Physics-Modelling'

# Get the parent directory. Should be the root of the repository
parent_dir = os.path.dirname(current_dir)

# The root of the repo should contain the incubator_dt folder. Otherwise something went wrong in 0-Pre-requisites.
assert os.path.exists(os.path.join(parent_dir, 'incubator_dt')), 'incubator_dt folder not found in the repository root'

incubator_dt_software_dir = os.path.join(parent_dir, 'incubator_dt', 'software')

assert os.path.exists(incubator_dt_software_dir), 'incubator_dt software directory not found'

# Add the parent directory to sys.path
sys.path.append(incubator_dt_software_dir)

from incubator.communication.shared.protocol import ROUTING_KEY_STATE, ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS
from incubator.communication.server.rabbitmq import Rabbitmq, from_ns_to_s

class NNBasedMonitoringService:
    def __init__(self, max_diff, safe_temp, rabbitmq_config):
        self._l = logging.getLogger("NNBasedMonitoringService")

        self._max_diff = max_diff # Maximum tolerated difference.
        assert self._max_diff > 0, "max_diff must be greater than 0."
        self._safe_temp = safe_temp # Safe temperature to set controller to.

        # Check that temperature_prediction_model.pth file exists:
        model_path = "temperature_prediction_model.pth"
        assert os.path.exists(model_path), f'{model_path} not found'
        self.temperature_prediction_model = IncubatorPredictionNN(model_path)
        self.rabbitmq = Rabbitmq(**rabbitmq_config)

        # current and previous inputs to NN
        self.past_time = None
        self.past_Tb = None
        self.past_room_Temp = None
        self.past_heater_state = None
        self.current_time = None
        self.current_Tb = None
        self.current_room_Temp = None
        self.current_heater_state = None
        self.predicted_Tb = None
        self.prediction_error = None

    def _record_message(self, message):
        # Check that time_step is still matching the dataset timestep. If the timestep changes, the NN has to be retrained.
        time_step = message['fields']['execution_interval'] # s

        assert abs(time_step - 3.0) < 0.1, "time_step does not match the dataset timestep. If the timestep changes, the NN has to be retrained."

        if self.current_time is None: # No message seen so far
            self._l.info(f"First message received.")
            assert self.past_time is None

            self.current_time = message['time'] # ns
            self.current_Tb = message['fields']['average_temperature']
            self.current_room_Temp = message['fields']['t3']
            self.current_heater_state = 1 if message["fields"]["heater_on"] else 0
        else:
            self.past_time = self.current_time
            self.past_Tb = self.current_Tb
            self.past_room_Temp = self.current_room_Temp
            self.past_heater_state = self.current_heater_state

            self.current_time = message['time'] # ns
            self.current_Tb = message['fields']['average_temperature']
            self.current_room_Temp = message['fields']['t3']
            self.current_heater_state = 1 if message["fields"]["heater_on"] else 0
            # Check that the messages are separated by time_step

            assert message['time'] >= self.current_time
            new_msg_time_diff = from_ns_to_s(message['time'] - self.current_time) # s
            new_msg_time_bigger_than_step = new_msg_time_diff > time_step + 0.1

            if new_msg_time_bigger_than_step:
                self._l.warn(f"New message received is too far apart from previous message: difference={new_msg_time_diff}s, last_timestamp={self.current_time}ns, new_timestamp={message['time']}ns")
                self.current_time = message['time'] # ns
                self.current_Tb = message['fields']['average_temperature']
                self.current_room_Temp = message['fields']['t3']
                self.current_heater_state = 1 if message["fields"]["heater_on"] else 0

                # Drop all records. This will force service to wait for 2 new messages that are contiguous.
                self.past_time = None
                self.past_Tb = None
                self.past_room_Temp = None
                self.past_heater_state = None

                self.current_time = None
                self.current_Tb = None
                self.current_room_Temp = None
                self.current_heater_state = None

    def setup(self):
        self.rabbitmq.connect_to_server()
        self.rabbitmq.subscribe(routing_key=ROUTING_KEY_STATE,
                                on_message_callback=self.control_loop_callback)
        self._l.info("setup complete.")

    def prediction_step(self):
        self._l.info("step")

        # Recall that the network predicts the next temperature, so we use the previous values to make the prediction.
        if self.current_time is not None and self.past_time is not None:
            assert self.current_Tb is not None
            assert self.past_Tb is not None
            assert self.past_room_Temp is not None
            assert self.past_heater_state is not None

            self.temperature_prediction_model.predict_Tb(self.past_Tb, self.past_room_Temp, self.past_heater_state)
            self.predicted_Tb = self.temperature_prediction_model.get_Tb()
            self.prediction_error = self.current_Tb - self.predicted_Tb
            self._l.debug(f"prediction step done. Time(ns): {self.current_time}, Heater: {self.past_heater_state}, Temp: {self.past_Tb}, Room Temp: {self.past_room_Temp}, New Temperature: {self.predicted_Tb}")
        else:
            self._l.debug(f"prediction not step done yet. Waiting for more data.")

    def cleanup(self):
        self.rabbitmq.close()

    def upload_state(self):
        if self.current_time is not None and self.past_time is not None:
            assert self.predicted_Tb is not None
            assert self.prediction_error is not None

            prediction_data = {
                "measurement": "temperature_prediction_service",
                "time": self.current_time,
                "tags": {
                    "source": "temperature_prediction_service"
                },
                "fields": {
                    "prediction_error": self.prediction_error,
                    "average_temperature": self.predicted_Tb,
                }
            }
            self.rabbitmq.send_message(routing_key="incubator.record.dtcourse.temperature_prediction_service.average_temperature", message=prediction_data)

    def reconfigure(self):
        if self.current_time is not None and self.past_time is not None:
            assert self.predicted_Tb is not None
            assert self.prediction_error is not None

            if abs(self.prediction_error) > self._max_diff:
                self._l.debug("Setting controller to safe mode.")
                self.rabbitmq.send_message(ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS, {
                    "temperature_desired": self._safe_temp,
                })

    def control_loop_callback(self, ch, method, properties, body_json):
        self._record_message(body_json)

        self.prediction_step()

        self.reconfigure()

        self.upload_state()

    def start(self):
        try:
            self.rabbitmq.start_consuming()
        except:
            self._l.warning("Stopping NNBasedMonitoringService")
            self.cleanup()
            raise

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
    service = NNBasedMonitoringService(max_diff=0.1, safe_temp = 25.0, rabbitmq_config=config["rabbitmq"])

    service.setup()

    # Start the NNBasedMonitoringService
    service.start()

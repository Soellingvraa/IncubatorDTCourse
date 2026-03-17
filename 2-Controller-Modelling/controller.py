
# Configure python path to load incubator modules
import sys
import os
import logging
import logging.config
import time

# Get the current working directory. Should be 2-Controller-Modelling
current_dir = os.getcwd()

assert os.path.basename(current_dir) == '2-Controller-Modelling', 'Current directory is not 2-Controller-Modelling'

# Get the parent directory. Should be the root of the repository
parent_dir = os.path.dirname(current_dir)

# The root of the repo should contain the incubator_dt folder. Otherwise something went wrong in 0-Pre-requisites.
assert os.path.exists(os.path.join(parent_dir, 'incubator_dt')), 'incubator_dt folder not found in the repository root'

incubator_dt_software_dir = os.path.join(parent_dir, 'incubator_dt', 'software')

assert os.path.exists(incubator_dt_software_dir), 'incubator_dt software directory not found'

# Add the parent directory to sys.path
sys.path.append(incubator_dt_software_dir)

import time
import sys
from datetime import datetime
import logging

from incubator.communication.server.rabbitmq import Rabbitmq, ROUTING_KEY_STATE, ROUTING_KEY_HEATER, ROUTING_KEY_FAN, \
    from_ns_to_s
from incubator.communication.shared.protocol import ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS
from incubator.models.controller_models.controller_model_sm import ControllerModel4SM

class ControllerFMU:
    """
    A simple implementation of an FMU in Python for co-simulation.
    This FMU represents a thermostat controller with the states: Heating, and Cooling.
    The system adjusts the temperature based on the current room temperature, target temperature, and max temperature.
    """

    def __init__(self, max_temp, min_temp, hysteresis=0.3):
        # Initial state
        self.state = "Heating"
        self.heater_on = True
        self.time = 0.0  # Simulation start time

        # Temperature variables
        self.current_temp = 20.0  # Initial room temperature (Celsius)
        self.max_temp = max_temp  # Maximum allowed temperature (Celsius)
        self.min_temp = min_temp  # Minimum allowed temperature (Celsius)
        self.hysteresis = hysteresis  # Safety margin to reduce overshoot/undershoot

    def fmi2Instantiate(self):
        """
        Simulate the FMU instantiation, which allocates resources and prepares the FMU.
        """
        print("ControllerFMU instantiated with initial state.")
        self.time = 0.0
        self.state = "Heating"
        self.current_temp = 20.0

    def fmi2SetupExperiment(self, start_time, stop_time):
        """
        Setup the simulation experiment.
        """
        print(f"FMU experiment setup: Start Time = {start_time}, Stop Time = {stop_time}")
        self.time = start_time

    def fmi2EnterInitializationMode(self):
        """
        FMU enters initialization mode. This is where initial conditions would be set.
        """
        print("ControllerFMU entered initialization mode.")

    def fmi2ExitInitializationMode(self):
        """
        FMU exits initialization mode and is ready to start the simulation.
        """
        print("ControllerFMU exited initialization mode.")

    def fmi2DoStep(self, current_time, step_size):
        """
        Perform one simulation step. This simulates the thermostat's state machine.
        """
        # Update the simulation time
        self.time = current_time + step_size

        if (self.max_temp - self.min_temp) <= 2 * self.hysteresis:
            raise ValueError("Temperature band too small for configured hysteresis")

        heat_off_threshold = self.max_temp - self.hysteresis
        heat_on_threshold = self.min_temp + self.hysteresis

        # State machine logic
        if self.state == "Heating":
            if self.current_temp >= heat_off_threshold:
                self.state = "Cooling"
                self.heater_on = False

        elif self.state == "Cooling":
            if self.current_temp <= heat_on_threshold:
                self.state = "Heating"
                self.heater_on = True

        print(f"Time: {self.time:.2f}s, State: {self.state}, Temp: {self.current_temp:.2f}°C")

    def fmi2Terminate(self):
        """
        Terminate the FMU simulation and free resources.
        """
        print("ControllerFMU simulation terminated.")

class ControllerService:
    def __init__(self, max_temp, min_temp, rabbit_config):
        self._l = logging.getLogger("ControllerService")

        self.time = 0 # ns
        self.heater_ctrl = None
        self.fan_ctrl = None
        self.state_machine = ControllerFMU(max_temp, min_temp)

        self.rabbitmq = Rabbitmq(**rabbit_config)

    def _record_message(self, message):
        self.time = message['time'] # ns
        self.time_step = message['fields']['execution_interval'] # s
        # Sets the input to the FMU
        self.state_machine.current_temp = message['fields']['average_temperature']

    def safe_protocol(self):
        self._l.debug("Stopping Fan")
        self.fan_ctrl = False
        self._set_fan_on(False)
        self._l.debug("Stopping Heater")
        self.heater_ctrl = False
        self._set_heater_on(False)

    def _set_heater_on(self, on):
        self.rabbitmq.send_message(routing_key=ROUTING_KEY_HEATER, message={"heater": on})

    def _set_fan_on(self, on):
        self.fan_ctrl = on
        self.rabbitmq.send_message(routing_key=ROUTING_KEY_FAN, message={"fan": on})

    def setup(self):
        self.rabbitmq.connect_to_server()
        self.safe_protocol()
        self.rabbitmq.subscribe(routing_key=ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS,
                                on_message_callback=self.update_parameters)
        self.rabbitmq.subscribe(routing_key=ROUTING_KEY_STATE,
                                on_message_callback=self.control_loop_callback)


        self.state_machine.fmi2Instantiate()
        self.state_machine.fmi2SetupExperiment(0, -1)
        self.state_machine.fmi2EnterInitializationMode()
        self.state_machine.fmi2ExitInitializationMode()

        self._l.info("Controller setup complete.")

    def ctrl_step(self):
        self._l.debug("Controller step")

        self.fan_ctrl = True
        time_seconds = from_ns_to_s(self.time)
        self.state_machine.fmi2DoStep(time_seconds, self.time_step)
        self.heater_ctrl = self.state_machine.heater_on
        self._l.debug(f"Controller step done. Time(ns): {self.time}, Heater: {self.heater_ctrl}, Fan: {self.fan_ctrl}, Temp: {self.state_machine.current_temp}, State: {self.state_machine.state}")

    def cleanup(self):
        self.safe_protocol()
        self.rabbitmq.close()

    def upload_state(self, data):
        ctrl_data = {
            "measurement": "dtcourse_controller",
            "time": self.time,
            "tags": {
                "source": "dtcourse_controller"
            },
            "fields": {
                "plant_time": data["time"],
                "heater_on": 1 if self.heater_ctrl else 0,
                "fan_on": 1 if data["fields"]["fan_on"] else 0,
                "current_state": self.state_machine.state,
                "max_temperature": self.state_machine.max_temp,
                "min_temperature": self.state_machine.min_temp,
            }
        }
        self.rabbitmq.send_message(routing_key="incubator.record.dtcourse.controller.state", message=ctrl_data)

    def update_parameters(self, ch, method, properties, body_json):
        self._l.debug("New parameter msg:")
        self._l.debug(body_json)
        if "max_temp" in body_json:
            self.state_machine.max_temp = body_json["max_temp"]
        if "min_temp" in body_json:
            self.state_machine.min_temp = body_json["min_temp"]
        self._l.info("Reconfiguration of controller parameters done.")

    def control_loop_callback(self, ch, method, properties, body_json):
        self._record_message(body_json)

        self.ctrl_step()

        self.upload_state(body_json)

        assert self.heater_ctrl is not None
        self._set_heater_on(self.heater_ctrl)
        self._set_fan_on(self.fan_ctrl)

    def start_control(self):
        try:
            self.rabbitmq.start_consuming()
        except:
            self._l.warning("Stopping controller")
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

    max_temp=39.0
    min_temp=36.0

    controller = ControllerService(max_temp, min_temp, rabbit_config=config["rabbitmq"])
    controller.setup()
    controller.start_control()

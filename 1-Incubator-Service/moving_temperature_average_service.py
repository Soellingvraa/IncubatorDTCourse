
# Configure python path to load incubator modules
import sys
import os
import logging
import logging.config
import time
from collections import deque

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

from incubator.communication.server.rpc_server import RPCServer
from incubator.communication.server.rpc_client import RPCClient
from incubator.communication.shared.protocol import ROUTING_KEY_STATE

class MovingAverageTemperatureService(RPCServer):
    """
    This is a hybrid service that maintains a moving average of the last N values of the temperature in the incubator. It communicates with the average service to get the average. In addition, as a server, it responds to requests to reset the moving average.

    """
    def __init__(self, N, rabbitmq_config):
        super().__init__(**rabbitmq_config)

        # Keeps another connection to make RPC calls to the average service
        self._rpc_client = RPCClient(**rabbitmq_config)
        self._values = deque() # keeps a queue of the last N values. Old values are on the left and new ones are on the right.
        self._N = N
        self._l = logging.getLogger("MovingAverageTemperatureService")

    def setup(self):
        """ 
        Setup the RabbitMQ connection and declare the routing_key (this is the topic that this server will listen to) and queue (the name of the queue where all messages addressed to routing_key will be placed in by the RabbitMQ server).

        We use the same name for both the routing_key and the queue name. This is not necessary, but it makes it easier to understand what is happening in the RabbitMQ server.        
        """
        super(MovingAverageTemperatureService, self).setup(routing_key='incubator.dtcourse.moving_temperature_average_service', queue_name='incubator.dtcourse.moving_temperature_average_service')

        # Subscribe to the temperature topic incoming from the incubator physical twin. The ROUTING_KEY_STATE is defined in the protocol module, to avoid typos. It's value should be similar to "incubator.record.driver.state"
        self.subscribe(routing_key=ROUTING_KEY_STATE,
                                on_message_callback=self.process_state_sample)

        self._rpc_client.connect_to_server()

        self._l.info(f"MovingAverageTemperatureService setup complete.")

    def process_state_sample(self, ch, method, properties, body_json):
        """ 
        This is the method that will be invoked by the RPCServer class when a message arrives in the RabbitMQ queue. The body_json is the message that was sent by the incubator physical twin. It should contain the temperature value as well as other sensor values.
        """

        # Log the values received.
        self._l.info(f"Received state sample: {body_json}")

        # Append the temperature value to the deque
        self._values.append(body_json["fields"]["average_temperature"])

        # If the deque is larger than N, pop the oldest element
        if len(self._values) > self._N:
            self._values.popleft()

        # Prepare the message to send to the average service
        arguments = {
            "values": list(self._values)
        }

        # Call the average service to get the average
        response = self._rpc_client.invoke_method("incubator.dtcourse.average_service", "compute_average", arguments)

        # Log the response
        self._l.info(f"average_service response: {response}")

        if "average" in response and "std_error" in response:
            # Add the average and standard error to the body_json
            moving_average = response["average"]
            moving_std_error = response["std_error"]

            # Prepare a message to publish under a new topic representing this service's data stream. We reuse the same timestamp from the original message, to enable easier traceability between outputs and inputs.
            timestamp = body_json["time"]

            # The format of this message will become later. This is just to allow a uniform way to send data to the database.
            message = {
                "measurement": "moving_temperature_average_service",
                "time": timestamp,
                "tags": {
                    "source": "moving_temperature_average_service"
                },
                "fields": {
                    "moving_average_temperature": moving_average,
                    "moving_std_error": moving_std_error,
                }
            }

            # Log message
            self._l.info(f"Sending message: {message}")

            # Publish the message to the RabbitMQ server
            self.send_message("incubator.record.dtcourse.moving_temperature_average_service.temperature_moving_average", message)
        else:
            self._l.error(f"Error: {response}")


    def reset_average(self, reply_fun):
        """ 
        This is the method that will be invoked by the RPCServer class when a message arrives in the RabbitMQ queue. The reply_fun is a function that we can call to send the results back to the client that sent the message.
        """

         # Log the values received.
        self._l.info(f"reset_average called.")

        # Pop all elements except the most recent one
        while len(self._values) > 1:
            self._values.popleft()

        assert len(self._values) == 1 or len(self._values)==0, f"Expected 1 or 0 elements in the deque. Found {len(self._values)} elements."

        success_msg = "Average reset successfully."

        self._l.info(success_msg)

        # Prepare the results to send back.
        result_msg = {
            "msg": success_msg,
        }

        # Send results back.
        reply_fun(result_msg)

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
    service = MovingAverageTemperatureService(N=10, rabbitmq_config=config["rabbitmq"])

    service.setup()

    # Start the MovingAverageTemperatureService
    service.start_serving()

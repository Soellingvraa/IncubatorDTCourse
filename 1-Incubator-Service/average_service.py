
# Configure python path to load incubator modules
import sys
import os
import logging
import logging.config
import time
import math

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

class AverageService(RPCServer):
    """
    This is a server service that computes the average of a given list of values.
    It extends the RPCServer class, which is a class that listens to a RabbitMQ queue and waits for messages to arrive, and hides much of the complexity of the server service. 
    All we need to do to implement the average service is implement a method called "compute_average" that takes a list of values and returns the average of those values. This method will be called by the RPCServer class when a message arrives in the RabbitMQ queue containing the name of the method to call and the arguments to pass to the method.
    """
    def __init__(self, rabbitmq_config):
        super().__init__(**rabbitmq_config)
        self._l = logging.getLogger("AverageService")

    def setup(self):
        """ 
        Setup the RabbitMQ connection and declare the routing_key (this is the topic that this server will listen to) and queue (the name of the queue where all messages addressed to routing_key will be placed in by the RabbitMQ server).

        We use the same name for both the routing_key and the queue name. This is not necessary, but it makes it easier to understand what is happening in the RabbitMQ server.        
        """
        super(AverageService, self).setup(routing_key='incubator.dtcourse.average_service', queue_name='incubator.dtcourse.average_service')

        self._l.info(f"AverageService setup complete.")

    def compute_average(self, values, reply_fun):
        """ 
        This is the method that will be invoked by the RPCServer class when a message arrives in the RabbitMQ queue. The reply_fun is a function that we can call to send the results back to the client that sent the message.
        """
        average = 0.0
        standard_error = 0.0

        # Log the values received.
        self._l.info(f"compute_average called. Received values: {values}")

        # Compute the average of the values.
        if len(values) > 0:
            average = sum(values) / len(values)

            # Standard error = sample standard deviation / sqrt(n).
            if len(values) > 1:
                sample_variance = sum((value - average) ** 2 for value in values) / (len(values) - 1)
                standard_error = math.sqrt(sample_variance) / math.sqrt(len(values))
            else:
                standard_error = 0.0
        else:
            self._l.warning("Received an empty list of values. Cannot compute average. Returning error")
            reply_fun({"error": "Received an empty list of values. Cannot compute average."})
            return

        # Prepare the results to send back.
        result_msg = {
            "average": average,
            "std_error": standard_error
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
    service = AverageService(rabbitmq_config=config["rabbitmq"])

    service.setup()

    # Start the AverageService
    service.start_serving()

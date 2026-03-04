# Configure python path to load incubator modules
import sys
import os

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

# Append the same path to PYTHONPATH
os.environ['PYTHONPATH'] = os.pathsep.join([os.environ.get('PYTHONPATH', ''), incubator_dt_software_dir])

from startup.start_docker_influxdb import start_docker_influxdb
from startup.start_docker_rabbitmq import start_docker_rabbitmq
from startup.start_incubator_realtime_mockup import start_incubator_realtime_mockup
from startup.start_influx_data_recorder import start_influx_data_recorder
from startup.start_low_level_driver_mockup import start_low_level_driver_mockup
from startup.start_controller_physical import start_controller_physical
from startup.utils.start_as_daemon import start_as_daemon

if __name__ == '__main__':
    start_docker_rabbitmq()
    start_docker_influxdb()

    start_as_daemon(start_incubator_realtime_mockup)
    start_as_daemon(start_low_level_driver_mockup)
    start_as_daemon(start_influx_data_recorder)
    start_as_daemon(start_controller_physical)

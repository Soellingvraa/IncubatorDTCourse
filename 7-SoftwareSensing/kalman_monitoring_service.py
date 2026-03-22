
# Configure python path to load incubator modules
import os
import sys
import logging

import numpy as np
from control import ss
from filterpy.common import Q_discrete_white_noise
from filterpy.kalman import KalmanFilter

# Get the current working directory. Should be 7-SoftwareSensing
current_dir = os.getcwd()
assert os.path.basename(current_dir) == "7-SoftwareSensing", "Current directory is not 7-SoftwareSensing"

# Get the parent directory. Should be the root of the repository
parent_dir = os.path.dirname(current_dir)
assert os.path.exists(os.path.join(parent_dir, "incubator_dt")), "incubator_dt folder not found in the repository root"

incubator_dt_software_dir = os.path.join(parent_dir, "incubator_dt", "software")
assert os.path.exists(incubator_dt_software_dir), "incubator_dt software directory not found"

# Add incubator_dt software path to sys.path
sys.path.append(incubator_dt_software_dir)

from incubator.communication.shared.protocol import ROUTING_KEY_STATE, ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS
from incubator.communication.server.rabbitmq import Rabbitmq, from_ns_to_s


class KalmanMonitoringService:
    def __init__(self, max_diff, safe_temp, rabbitmq_config, plant_params, step_size=3.0):
        self._l = logging.getLogger("KalmanMonitoringService")
        self._max_diff = max_diff
        self._safe_temp = safe_temp
        self._step_size = step_size
        self.rabbitmq = Rabbitmq(**rabbitmq_config)

        assert self._max_diff > 0, "max_diff must be greater than 0."
        assert self._step_size > 0, "step_size must be greater than 0."

        self._build_filter(plant_params)

        self.current_time = None
        self.current_Tb = None
        self.current_room_temp = None
        self.current_heater_state = None

        self.predicted_Tb_prior = None
        self.predicted_Theater_prior = None
        self.prediction_error = None

    def _build_filter(self, plant_params):
        c_air = float(plant_params["C_air"])
        g_box = float(plant_params["G_box"])
        c_heater = float(plant_params["C_heater"])
        g_heater = float(plant_params["G_heater"])
        v_heater = float(plant_params["V_heater"])
        i_heater = float(plant_params["I_heater"])

        initial_heat_temperature = float(plant_params.get("initial_heat_temperature", 21.0))
        initial_box_temperature = float(plant_params.get("initial_box_temperature", 21.0))

        # Continuous-time state-space system.
        # x = [T_heater, T_box], u = [heater_on, T_room], y = [T_box]
        a_num = np.array([
            [-g_heater / c_heater, g_heater / c_heater],
            [g_heater / c_air, -(g_heater + g_box) / c_air],
        ], dtype=np.float64)

        b_num = np.array([
            [v_heater * i_heater / c_heater, 0.0],
            [0.0, g_box / c_air],
        ], dtype=np.float64)

        c_num = np.array([[0.0, 1.0]], dtype=np.float64)
        d_num = np.array([[0.0, 0.0]], dtype=np.float64)

        dt_system = ss(a_num, b_num, c_num, d_num).sample(self._step_size, method="backward_diff")

        std_dev_measurement_noise = 0.9
        std_dev_process_noise = 0.1
        process_covariance_init = np.array([[100.0, 0.0], [0.0, 100.0]], dtype=np.float64)

        self.f = KalmanFilter(dim_x=2, dim_z=1, dim_u=2)
        self.f.x = np.array([[initial_heat_temperature], [initial_box_temperature]], dtype=np.float64)
        self.f.F = dt_system.A
        self.f.B = dt_system.B
        self.f.H = dt_system.C
        self.f.P = process_covariance_init
        self.f.R = np.array([[std_dev_measurement_noise]], dtype=np.float64)
        self.f.Q = Q_discrete_white_noise(dim=2, dt=self._step_size, var=std_dev_process_noise ** 2)

    def _record_message(self, message):
        fields = message["fields"]
        measured_interval = float(fields["execution_interval"])
        if abs(measured_interval - self._step_size) > 0.2:
            self._l.warning(
                f"Unexpected execution interval {measured_interval}s; expected {self._step_size}s."
            )

        prev_time = self.current_time
        prev_Tb = self.current_Tb

        self.current_time = int(message["time"])
        self.current_Tb = float(fields["average_temperature"])
        self.current_room_temp = float(fields.get("t3", fields.get("t1", self.current_Tb)))
        self.current_heater_state = 1.0 if fields.get("heater_on", False) else 0.0

        if prev_time is not None:
            dt = from_ns_to_s(self.current_time - prev_time)
            if dt > measured_interval + 0.2:
                self._l.warning(f"Data gap detected ({dt:.2f}s). Reinitializing KF state.")
                self.f.x = np.array([[prev_Tb], [self.current_Tb]], dtype=np.float64)
                self.f.P = np.array([[100.0, 0.0], [0.0, 100.0]], dtype=np.float64)

    def setup(self):
        self.rabbitmq.connect_to_server()
        self.rabbitmq.subscribe(routing_key=ROUTING_KEY_STATE, on_message_callback=self.control_loop_callback)
        self._l.info("setup complete")

    def prediction_step(self):
        if self.current_time is None:
            return

        control_u = np.array([[self.current_heater_state], [self.current_room_temp]], dtype=np.float64)
        measurement = np.array([[self.current_Tb]], dtype=np.float64)

        self.f.predict(u=control_u)
        self.predicted_Theater_prior = float(self.f.x_prior[0, 0])
        self.predicted_Tb_prior = float(self.f.x_prior[1, 0])
        self.prediction_error = self.current_Tb - self.predicted_Tb_prior

        self.f.update(measurement)

    def upload_state(self):
        if self.current_time is None or self.prediction_error is None:
            return

        kf_data = {
            "measurement": "kalman_monitoring_service",
            "time": self.current_time,
            "tags": {"source": "kalman_monitoring_service"},
            "fields": {
                "predicted_temperature": self.predicted_Tb_prior,
                "predicted_heater_temperature": self.predicted_Theater_prior,
                "estimated_temperature": float(self.f.x[1, 0]),
                "estimated_heater_temperature": float(self.f.x[0, 0]),
                "prediction_error": float(self.prediction_error),
                "sigma_temperature": float(np.sqrt(self.f.P[1, 1])),
                "sigma_heater_temperature": float(np.sqrt(self.f.P[0, 0])),
            },
        }
        self.rabbitmq.send_message(
            routing_key="incubator.record.dtcourse.kalman_monitoring_service.state",
            message=kf_data,
        )

    def reconfigure(self):
        if self.prediction_error is None:
            return

        if abs(self.prediction_error) > self._max_diff:
            self._l.debug("Prediction mismatch too high: switching controller to safe mode")
            self.rabbitmq.send_message(
                ROUTING_KEY_UPDATE_CLOSED_CTRL_PARAMS,
                {"temperature_desired": self._safe_temp},
            )

    def control_loop_callback(self, ch, method, properties, body_json):
        self._record_message(body_json)
        self.prediction_step()
        self.reconfigure()
        self.upload_state()

    def cleanup(self):
        self.rabbitmq.close()

    def start(self):
        try:
            self.rabbitmq.start_consuming()
        except Exception:
            self._l.warning("Stopping KalmanMonitoringService")
            self.cleanup()
            raise


if __name__ == "__main__":
    from pyhocon import ConfigFactory

    logging.basicConfig(level=logging.INFO)

    startup_conf = os.path.join(parent_dir, "incubator_dt", "software", "startup.conf")
    assert os.path.exists(startup_conf), "startup.conf file not found"

    config = ConfigFactory.parse_file(startup_conf)
    plant_params = config["digital_twin"]["models"]["plant"]["param4"]

    service = KalmanMonitoringService(
        max_diff=0.5,
        safe_temp=25.0,
        rabbitmq_config=config["rabbitmq"],
        plant_params=plant_params,
        step_size=3.0,
    )
    service.setup()
    service.start()

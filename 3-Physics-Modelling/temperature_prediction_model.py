import torch
import torch.nn as nn
import torch.optim as optim

# Define the neural network model (a simple linear regression model). Note that this much match the model architecture used in training
class LinearRegression(nn.Module):
    def __init__(self):
        super(LinearRegression, self).__init__()
        self.linear = nn.Linear(3, 1)  # 3 input features and 1 output

    def forward(self, x):
        return self.linear(x)

class IncubatorPredictionNN:
    def __init__(self, model_path='temperature_prediction_model.pth'):
        # Instantiate the model
        self.model = LinearRegression()

        # Load the saved state dictionary into the model
        self.model.load_state_dict(torch.load(model_path))

        # Set the model to evaluation mode (important for inference)
        self.model.eval()

        self.Tb = 0.0

    def predict_Tb(self, Tb, Tr, H_h):
        # Prepare the inputs to the NN model
        nn_input = torch.tensor([[Tb, Tr, H_h]], dtype=torch.float32)

        # Make the prediction with the neural network
        with torch.no_grad():  # No need to compute gradients during inference
            self.Tb = self.model(nn_input).item()

    def get_Tb(self):
        return self.Tb

if __name__ == "__main__":
    # Initialize the incubator prediction model
    # This will load the neural network model from 'temperature_prediction_model.pth'
    model_path = 'temperature_prediction_model.pth'
    incubator_nn = IncubatorPredictionNN(model_path)

    # Provide a set of inputs: 
    # Tb: the current box temperature,
    # Tr: the room temperature,
    # H_h: the heater state (1 for ON, 0 for OFF)
    current_Tb = 25.0  # Current box temperature (in °C)
    room_Temp = 22.0   # Room temperature (in °C)
    heater_state = 1   # Heater state (1: ON, 0: OFF)

    # Predict the next box temperature based on the input data
    incubator_nn.predict_Tb(current_Tb, room_Temp, heater_state)

    # Retrieve and print the predicted temperature
    predicted_Tb = incubator_nn.get_Tb()
    print(f"Predicted next box temperature: {predicted_Tb:.2f} °C")

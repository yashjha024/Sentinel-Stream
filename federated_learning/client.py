import torch
import torch.nn as nn
import torch.optim as optim
from collections import OrderedDict
import copy

class FedClient:
    def __init__(self, client_id, model, train_loader, test_loader, device):
        self.client_id = client_id
        self.model = model.to(device)
        self.train_loader = train_loader
        self.test_loader = test_loader
        self.device = device
        self.criterion = nn.CrossEntropyLoss()
    

        
    def set_parameters(self, parameters):
        """Set model parameters from server"""
        params_dict = zip(self.model.state_dict().keys(), parameters)
        state_dict = OrderedDict({k: torch.tensor(v) for k, v in params_dict})
        self.model.load_state_dict(state_dict, strict=True)
    
    def get_parameters(self):
        """Get model parameters"""
        return [val.cpu().numpy() for _, val in self.model.state_dict().items()]
    
    def train(self, epochs=5, lr=0.001):
        """Train the model locally"""
        optimizer = optim.Adam(self.model.parameters(), lr=lr)
        self.model.train()
        
        total_loss = 0
        total_samples = 0
        
        for epoch in range(epochs):
            for batch_idx, (data, target) in enumerate(self.train_loader):
                data, target = data.to(self.device), target.to(self.device)
                
                optimizer.zero_grad()
                output = self.model(data)
                loss = self.criterion(output, target)
                loss.backward()
                optimizer.step()
                
                total_loss += loss.item() * len(data)
                total_samples += len(data)
        
        avg_loss = total_loss / total_samples
        return avg_loss, total_samples 

    
    

    
    def evaluate(self):
        """Evaluate the model"""
        self.model.eval()
        total_loss = 0
        correct = 0
        total_samples = 0
        
        with torch.no_grad():
            for data, target in self.test_loader:
                data, target = data.to(self.device), target.to(self.device)
                output = self.model(data)
                loss = self.criterion(output, target)
                
                total_loss += loss.item() * len(data)
                pred = output.argmax(dim=1, keepdim=True)
                correct += pred.eq(target.view_as(pred)).sum().item()
                total_samples += len(data)
        
        avg_loss = total_loss / total_samples
        accuracy = correct / total_samples
        return avg_loss, accuracy 
    
    
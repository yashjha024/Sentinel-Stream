import torch
import numpy as np
from collections import OrderedDict
import copy

class FedAdamServer:
    def __init__(self, model, beta1=0.9, beta2=0.999, eta=0.001, tau=1e-3):
        self.global_model = model
        self.beta1 = beta1
        self.beta2 = beta2
        self.eta = eta  # server learning rate
        self.tau = tau  # regularization parameter
        
        # Initialize momentum and velocity for FedAdam
        self.m = [torch.zeros_like(param) for param in model.parameters()]
        self.v = [torch.zeros_like(param) for param in model.parameters()]
        self.round = 0
        
    def get_parameters(self):
        """Get global model parameters"""
        return [val.cpu().numpy() for _, val in self.global_model.state_dict().items()]
    
    def set_parameters(self, parameters):
        """Set global model parameters"""
        params_dict = zip(self.global_model.state_dict().keys(), parameters)
        state_dict = OrderedDict({k: torch.tensor(v) for k, v in params_dict})
        self.global_model.load_state_dict(state_dict, strict=True)
    
    def aggregate_fit(self, results):
        """FedAdam aggregation"""
        if not results:
            return self.get_parameters()
        
        self.round += 1
        
        # Calculate weighted average of client updates
        total_samples = sum([num_samples for _, num_samples, _ in results])
        
        # Initialize aggregated gradients
        aggregated_grads = [torch.zeros_like(param) for param in self.global_model.parameters()]
        
        current_params = list(self.global_model.parameters())
        
        for client_params, num_samples, _ in results:
            weight = num_samples / total_samples
            
            # Convert client parameters to tensors
            client_tensors = [torch.tensor(param) for param in client_params]
            
            # Calculate pseudo-gradients (difference between global and client parameters)
            for i, (global_param, client_param) in enumerate(zip(current_params, client_tensors)):
                pseudo_grad = global_param - client_param
                aggregated_grads[i] += weight * pseudo_grad
        
        # Apply FedAdam update
        updated_params = []
        for i, (param, grad) in enumerate(zip(current_params, aggregated_grads)):
            # Update biased first moment estimate
            self.m[i] = self.beta1 * self.m[i] + (1 - self.beta1) * grad
            
            # Update biased second raw moment estimate
            self.v[i] = self.beta2 * self.v[i] + (1 - self.beta2) * (grad ** 2)
            
            # Compute bias-corrected first moment estimate
            m_hat = self.m[i] / (1 - self.beta1 ** self.round)
            
            # Compute bias-corrected second raw moment estimate
            v_hat = self.v[i] / (1 - self.beta2 ** self.round)
            
            # Update parameters
            updated_param = param - self.eta * m_hat / (torch.sqrt(v_hat) + self.tau)
            updated_params.append(updated_param)
        
        # Update global model
        with torch.no_grad():
            for param, updated_param in zip(self.global_model.parameters(), updated_params):
                param.copy_(updated_param)
        
        return self.get_parameters()
    
    def aggregate_evaluate(self, results):
        """Aggregate evaluation results"""
        if not results:
            return 0.0, 0.0
        
        total_samples = sum([num_samples for _, _, num_samples in results])
        weighted_loss = sum([loss * num_samples for loss, _, num_samples in results]) / total_samples
        weighted_accuracy = sum([acc * num_samples for _, acc, num_samples in results]) / total_samples
        
        return weighted_loss, weighted_accuracy

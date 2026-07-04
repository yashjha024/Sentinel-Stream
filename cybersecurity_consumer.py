import json
import torch
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime
import pickle
from collections import Counter, deque
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import threading
import time

# Import your federated learning model
from federated_learning.models import CyberSecurityNet

class RealTimeThreatDetector:
    def __init__(self):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            'cybersecurity-stream',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8'),
            auto_offset_reset='latest',  # Start from latest messages
            group_id='threat-detection-group'
        )
        
        # Load your trained federated learning model
        self.load_trained_model()
        
        # Real-time analytics
        self.threat_counter = Counter()
        self.recent_threats = deque(maxlen=100)  # Keep last 100 threats
        self.message_count = 0
        self.start_time = time.time()
        
        # Thread safety
        self.data_lock = threading.Lock()
        
        print("🛡️  Real-Time Threat Detection System initialized!")
        print("🔍 Monitoring cybersecurity-stream topic...")
    
    def load_trained_model(self):
        """Load your trained federated learning model"""
        try:
            # Load the model checkpoint
            checkpoint = torch.load('federated_model_checkpoint.pth', map_location='cpu')
            
            # Initialize model with saved parameters
            self.model = CyberSecurityNet(
                input_dim=checkpoint['input_dim'],
                num_classes=checkpoint['num_classes']
            )
            self.model.load_state_dict(checkpoint['model_state_dict'])
            self.model.eval()
            
            # Load preprocessors
            self.scaler_maj = checkpoint['scaler_maj']
            self.scaler_min = checkpoint['scaler_min']
            self.label_encoder_maj = checkpoint['label_encoder_maj']
            self.label_encoder_min = checkpoint['label_encoder_min']
            
            print("✅ Loaded trained federated learning model")
            
        except FileNotFoundError:
            print("⚠️  Model checkpoint not found, using dummy classifier")
            self.model = None
    
    def preprocess_data(self, record):
        """Preprocess incoming data for model prediction"""
        try:
            # Convert record to DataFrame
            df = pd.DataFrame([record])
            
            # Remove non-feature columns
            feature_cols = [col for col in df.columns if col not in ['Label', 'timestamp', 'message_id']]
            X = df[feature_cols]
            
            # Handle categorical variables (basic encoding)
            for col in X.select_dtypes(include=['object']).columns:
                X[col] = pd.Categorical(X[col]).codes
            
            # Scale features (use majority scaler as default)
            X_scaled = self.scaler_maj.transform(X.fillna(0))
            
            return torch.FloatTensor(X_scaled)
            
        except Exception as e:
            print(f"❌ Preprocessing error: {e}")
            return None
    
    def predict_threat(self, record):
        """Predict if incoming data is a threat"""
        if self.model is None:
            # Dummy prediction if model not loaded
            return record.get('Label', 'Normal'), 0.5
        
        try:
            # Preprocess data
            X = self.preprocess_data(record)
            if X is None:
                return 'Normal', 0.0
            
            # Make prediction
            with torch.no_grad():
                output = self.model(X)
                probabilities = torch.softmax(output, dim=1)
                predicted_class = output.argmax(dim=1).item()
                confidence = probabilities.max().item()
            
            # Map prediction to label (simplified)
            threat_labels = ['Normal', 'DDoS', 'Probe', 'DoS', 'BFA', 'Web-Attack', 'BOTNET', 'U2R']
            predicted_label = threat_labels[min(predicted_class, len(threat_labels)-1)]
            
            return predicted_label, confidence
            
        except Exception as e:
            print(f"❌ Prediction error: {e}")
            return 'Normal', 0.0
    
    def process_message(self, message):
        """Process each incoming message"""
        try:
            record = message.value
            
            # Make threat prediction
            predicted_threat, confidence = self.predict_threat(record)
            
            # Update analytics
            with self.data_lock:
                self.message_count += 1
                self.threat_counter[predicted_threat] += 1
                
                # Store recent threat info
                threat_info = {
                    'timestamp': datetime.now(),
                    'predicted_threat': predicted_threat,
                    'confidence': confidence,
                    'src_ip': record.get('src_ip', 'unknown'),
                    'actual_label': record.get('Label', 'unknown')
                }
                self.recent_threats.append(threat_info)
            
            # Alert on high-risk threats
            if predicted_threat != 'Normal' and confidence > 0.7:
                print(f"🚨 HIGH RISK THREAT DETECTED!")
                print(f"   Type: {predicted_threat}")
                print(f"   Confidence: {confidence:.3f}")
                print(f"   Source: {record.get('src_ip', 'unknown')}")
                print(f"   Time: {datetime.now().strftime('%H:%M:%S')}")
                print("-" * 50)
            
            # Print periodic stats
            if self.message_count % 20 == 0:
                self.print_statistics()
                
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def print_statistics(self):
        """Print real-time statistics"""
        with self.data_lock:
            runtime = time.time() - self.start_time
            rate = self.message_count / runtime if runtime > 0 else 0
            
            print(f"\n📊 REAL-TIME STATISTICS (Runtime: {runtime:.1f}s)")
            print(f"   Messages processed: {self.message_count}")
            print(f"   Processing rate: {rate:.1f} msg/sec")
            print(f"   Threat distribution:")
            
            for threat, count in self.threat_counter.most_common():
                percentage = (count / self.message_count) * 100
                print(f"     {threat}: {count} ({percentage:.1f}%)")
            print("-" * 50)
    
    def start_monitoring(self):
        """Start real-time monitoring"""
        print("🔍 Starting real-time threat monitoring...")
        print("Press Ctrl+C to stop")
        
        try:
            for message in self.consumer:
                self.process_message(message)
                
        except KeyboardInterrupt:
            print("\n🛑 Monitoring stopped by user")
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        finally:
            self.consumer.close()
            self.print_final_report()
    
    def print_final_report(self):
        """Print final analysis report"""
        print(f"\n{'='*60}")
        print(f"📋 FINAL THREAT ANALYSIS REPORT")
        print(f"{'='*60}")
        
        with self.data_lock:
            total_runtime = time.time() - self.start_time
            
            print(f"Total monitoring time: {total_runtime:.1f} seconds")
            print(f"Total messages analyzed: {self.message_count}")
            print(f"Average processing rate: {self.message_count/total_runtime:.1f} msg/sec")
            
            print(f"\n🎯 Threat Detection Summary:")
            for threat, count in self.threat_counter.most_common():
                percentage = (count / self.message_count) * 100
                print(f"   {threat}: {count} detections ({percentage:.1f}%)")
            
            # Show recent high-confidence threats
            high_conf_threats = [t for t in self.recent_threats 
                               if t['predicted_threat'] != 'Normal' and t['confidence'] > 0.7]
            
            if high_conf_threats:
                print(f"\n🚨 Recent High-Confidence Threats:")
                for threat in high_conf_threats[-5:]:  # Show last 5
                    print(f"   {threat['timestamp'].strftime('%H:%M:%S')} - "
                          f"{threat['predicted_threat']} "
                          f"(conf: {threat['confidence']:.3f}) "
                          f"from {threat['src_ip']}")

if __name__ == "__main__":
    # Create and start the threat detector
    detector = RealTimeThreatDetector()
    detector.start_monitoring()

import json
import torch
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from datetime import datetime
import pickle
from collections import Counter, deque
import threading
import time

# Import your federated learning model
from federated_learning.models import CyberSecurityNet

class ThreatAlertSystem:
    def __init__(self):
        self.alert_thresholds = {
            'DDoS': 0.8,      # High confidence needed
            'U2R': 0.7,       # Critical - lower threshold
            'DoS': 0.8,
            'Web-Attack': 0.75,
            'BFA': 0.7,
            'BOTNET': 0.8
        }
        
        self.critical_threats = ['DDoS', 'U2R', 'BOTNET']
        
    def should_alert(self, threat_type, confidence):
        if threat_type in self.critical_threats:
            return confidence > self.alert_thresholds.get(threat_type, 0.5)
        return confidence > 0.8
    
    def send_alert(self, threat_info):
        print(f"\n🚨🚨 CRITICAL SECURITY ALERT 🚨🚨")
        print(f"Threat: {threat_info['type']}")
        print(f"Confidence: {threat_info['confidence']:.3f}")
        print(f"Source IP: {threat_info['src_ip']}")
        print(f"Timestamp: {threat_info['timestamp']}")
        print(f"Recommended Action: IMMEDIATE INVESTIGATION")
        print("="*60)

class EnhancedThreatDetector:
    def __init__(self):
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            'cybersecurity-stream',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8'),
            auto_offset_reset='latest',
            group_id='enhanced-threat-detection-group'
        )
        
        # Load your trained federated learning model
        self.load_trained_model()
        
        # Initialize alert system
        self.alert_system = ThreatAlertSystem()
        
        # Real-time analytics
        self.threat_counter = Counter()
        self.recent_threats = deque(maxlen=100)
        self.message_count = 0
        self.start_time = time.time()
        self.high_confidence_alerts = 0
        
        # Thread safety
        self.data_lock = threading.Lock()
        
        print("🛡️  Enhanced Real-Time Threat Detection System initialized!")
        print("🔍 Monitoring cybersecurity-stream topic with advanced alerting...")
    
    def load_trained_model(self):
        """Load your trained federated learning model"""
        try:
            checkpoint = torch.load('federated_model_checkpoint.pth', map_location='cpu')
            
            self.model = CyberSecurityNet(
                input_dim=checkpoint['input_dim'],
                num_classes=checkpoint['num_classes']
            )
            self.model.load_state_dict(checkpoint['model_state_dict'])
            self.model.eval()
            
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
            df = pd.DataFrame([record])
            feature_cols = [col for col in df.columns if col not in ['Label', 'timestamp', 'message_id']]
            X = df[feature_cols]
            
            for col in X.select_dtypes(include=['object']).columns:
                X[col] = pd.Categorical(X[col]).codes
            
            X_scaled = self.scaler_maj.transform(X.fillna(0))
            return torch.FloatTensor(X_scaled)
            
        except Exception as e:
            print(f"❌ Preprocessing error: {e}")
            return None
    
    def predict_threat(self, record):
        """Predict if incoming data is a threat"""
        if self.model is None:
            return record.get('Label', 'Normal'), 0.5
        
        try:
            X = self.preprocess_data(record)
            if X is None:
                return 'Normal', 0.0
            
            with torch.no_grad():
                output = self.model(X)
                probabilities = torch.softmax(output, dim=1)
                predicted_class = output.argmax(dim=1).item()
                confidence = probabilities.max().item()
            
            threat_labels = ['Normal', 'DDoS', 'Probe', 'DoS', 'BFA', 'Web-Attack', 'BOTNET', 'U2R']
            predicted_label = threat_labels[min(predicted_class, len(threat_labels)-1)]
            
            return predicted_label, confidence
            
        except Exception as e:
            print(f"❌ Prediction error: {e}")
            return 'Normal', 0.0
    
    def process_message(self, message):
        """Process each incoming message with enhanced monitoring"""
        try:
            record = message.value
            predicted_threat, confidence = self.predict_threat(record)
            
            # Update analytics
            with self.data_lock:
                self.message_count += 1
                self.threat_counter[predicted_threat] += 1
                
                threat_info = {
                    'timestamp': datetime.now(),
                    'predicted_threat': predicted_threat,
                    'confidence': confidence,
                    'src_ip': record.get('src_ip', 'unknown'),
                    'actual_label': record.get('Label', 'unknown'),
                    'type': predicted_threat
                }
                self.recent_threats.append(threat_info)
            
            # Enhanced alerting system
            if predicted_threat != 'Normal':
                if self.alert_system.should_alert(predicted_threat, confidence):
                    self.alert_system.send_alert({
                        'type': predicted_threat,
                        'confidence': confidence,
                        'src_ip': record.get('src_ip', 'unknown'),
                        'timestamp': datetime.now().strftime('%H:%M:%S')
                    })
                    self.high_confidence_alerts += 1
            
            # Enhanced statistics every 20 messages
            if self.message_count % 20 == 0:
                self.print_enhanced_statistics()
                
        except Exception as e:
            print(f"❌ Error processing message: {e}")
    
    def print_enhanced_statistics(self):
        """Enhanced statistics with security insights"""
        with self.data_lock:
            runtime = time.time() - self.start_time
            rate = self.message_count / runtime if runtime > 0 else 0
            
            print(f"\n{'='*70}")
            print(f"🛡️  ENHANCED CYBERSECURITY MONITORING DASHBOARD")
            print(f"{'='*70}")
            print(f"⏱️  Runtime: {runtime:.1f}s | Messages: {self.message_count} | Rate: {rate:.1f} msg/sec")
            print(f"🚨 High-Confidence Alerts: {self.high_confidence_alerts}")
            
            # Threat analysis
            total_threats = sum(count for threat, count in self.threat_counter.items() if threat != 'Normal')
            threat_percentage = (total_threats / self.message_count) * 100 if self.message_count > 0 else 0
            
            print(f"🚨 Threat Level: {threat_percentage:.1f}% ({total_threats}/{self.message_count})")
            
            if threat_percentage > 15:
                print(f"⚠️  HIGH THREAT ENVIRONMENT - INCREASED MONITORING RECOMMENDED")
            elif threat_percentage > 5:
                print(f"⚠️  MODERATE THREAT LEVEL - NORMAL VIGILANCE")
            else:
                print(f"✅ LOW THREAT ENVIRONMENT - NORMAL OPERATIONS")
            
            print(f"\n📊 Detailed Threat Breakdown:")
            for threat, count in self.threat_counter.most_common():
                percentage = (count / self.message_count) * 100
                severity = "🚨" if threat in ['DDoS', 'U2R', 'BOTNET'] else "⚠️" if threat != 'Normal' else "✅"
                print(f"   {severity} {threat}: {count} ({percentage:.1f}%)")
            
            print(f"{'='*70}")
    
    def export_results(self):
        """Export monitoring results to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export to CSV
        results_df = pd.DataFrame([
            {
                'timestamp': t['timestamp'],
                'threat_type': t['predicted_threat'],
                'confidence': t['confidence'],
                'src_ip': t['src_ip'],
                'actual_label': t['actual_label']
            }
            for t in self.recent_threats
        ])
        
        results_df.to_csv(f'threat_detection_results_{timestamp}.csv', index=False)
        
        # Export summary
        summary = {
            'total_messages': self.message_count,
            'runtime_seconds': time.time() - self.start_time,
            'threat_distribution': dict(self.threat_counter),
            'high_confidence_alerts': self.high_confidence_alerts,
            'threat_percentage': (sum(count for threat, count in self.threat_counter.items() 
                                    if threat != 'Normal') / self.message_count) * 100
        }
        
        with open(f'monitoring_summary_{timestamp}.json', 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"📁 Results exported to threat_detection_results_{timestamp}.csv")
    
    def start_monitoring(self):
        """Start enhanced real-time monitoring"""
        print("🔍 Starting enhanced threat monitoring...")
        print("Press Ctrl+C to stop")
        
        try:
            for message in self.consumer:
                self.process_message(message)
                
        except KeyboardInterrupt:
            print("\n🛑 Enhanced monitoring stopped by user")
        except Exception as e:
            print(f"❌ Consumer error: {e}")
        finally:
            self.consumer.close()
            self.export_results()
            self.print_final_report()
    
    def print_final_report(self):
        """Print final enhanced analysis report"""
        print(f"\n{'='*60}")
        print(f"📋 FINAL ENHANCED THREAT ANALYSIS REPORT")
        print(f"{'='*60}")
        
        with self.data_lock:
            total_runtime = time.time() - self.start_time
            
            print(f"Total monitoring time: {total_runtime:.1f} seconds")
            print(f"Total messages analyzed: {self.message_count}")
            print(f"High-confidence alerts generated: {self.high_confidence_alerts}")
            print(f"Average processing rate: {self.message_count/total_runtime:.1f} msg/sec")

if __name__ == "__main__":
    detector = EnhancedThreatDetector()
    detector.start_monitoring()

import pandas as pd
import numpy as np
import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime
import pickle

class CybersecurityDataProducer:
    def __init__(self):
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8')
        )
        
        # Load your actual data for realistic simulation
        self.load_sample_data()
        
        # Attack types from your federated learning model
        self.attack_types = ['Normal', 'DDoS', 'Probe', 'DoS', 'BFA', 'Web-Attack', 'BOTNET', 'U2R']
        
        print("🚀 Cybersecurity Data Producer initialized!")
        print(f"📊 Loaded {len(self.sample_data)} sample records")
    
    def load_sample_data(self):
        """Load sample data from your existing datasets"""
        try:
            # Load a small sample from your majority dataset
            majority_data = pd.read_csv('output/majority.csv').sample(n=1000, random_state=42)
            minority_data = pd.read_csv('output/minority.csv').sample(n=100, random_state=42)
            
            # Combine and shuffle
            self.sample_data = pd.concat([majority_data, minority_data]).sample(frac=1).reset_index(drop=True)
            print("✅ Loaded real cybersecurity data for simulation")
            
        except FileNotFoundError:
            print("⚠️  Original data files not found, generating synthetic data")
            self.generate_synthetic_data()
    
    def generate_synthetic_data(self):
        """Generate synthetic cybersecurity data if original files not available"""
        data = []
        for _ in range(1000):
            record = {
                'src_ip': f"192.168.{random.randint(1,255)}.{random.randint(1,255)}",
                'dst_ip': f"10.0.{random.randint(1,255)}.{random.randint(1,255)}",
                'src_port': random.randint(1024, 65535),
                'dst_port': random.choice([80, 443, 22, 21, 25, 53, 3389]),
                'protocol': random.choice(['TCP', 'UDP', 'ICMP']),
                'packet_size': random.randint(64, 1500),
                'duration': random.uniform(0.1, 300.0),
                'bytes_sent': random.randint(100, 10000),
                'bytes_received': random.randint(50, 5000),
                'Label': random.choice(self.attack_types)
            }
            data.append(record)
        
        self.sample_data = pd.DataFrame(data)
    
    def simulate_network_traffic(self, duration_minutes=10):
        """Simulate real-time network traffic"""
        print(f"🌐 Starting network traffic simulation for {duration_minutes} minutes...")
        print("📡 Sending data to Kafka topic: 'cybersecurity-stream'")
        
        start_time = time.time()
        message_count = 0
        
        while time.time() - start_time < duration_minutes * 60:
            # Select random record from sample data
            record_idx = random.randint(0, len(self.sample_data) - 1)
            record = self.sample_data.iloc[record_idx].to_dict()
            
            # Add real-time timestamp
            record['timestamp'] = datetime.now().isoformat()
            record['message_id'] = message_count
            
            # Simulate different traffic patterns
            if random.random() < 0.1:  # 10% chance of attack
                record['Label'] = random.choice(['DDoS', 'Probe', 'DoS', 'BFA', 'Web-Attack', 'BOTNET', 'U2R'])
                print(f"🚨 ATTACK DETECTED: {record['Label']} from {record.get('src_ip', 'unknown')}")
            else:
                record['Label'] = 'Normal'
            
            # Send to Kafka
            self.producer.send(
                topic='cybersecurity-stream',
                key=str(message_count),
                value=record
            )
            
            message_count += 1
            
            # Print progress every 10 messages
            if message_count % 10 == 0:
                print(f"📤 Sent {message_count} messages | Latest: {record['Label']}")
            
            # Simulate realistic timing (1-5 messages per second)
            time.sleep(random.uniform(0.2, 1.0))
        
        print(f"✅ Simulation completed! Sent {message_count} messages")
        self.producer.close()

if __name__ == "__main__":
    # Create and run the producer
    producer = CybersecurityDataProducer()
    
    # Start simulation (run for 5 minutes)
    producer.simulate_network_traffic(duration_minutes=5)

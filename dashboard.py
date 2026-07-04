import matplotlib.pyplot as plt
import matplotlib.animation as animation
from collections import deque, Counter
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import json
import threading
import numpy as np

class RealTimeDashboard:
    def __init__(self):
        # Initialize Kafka consumer for dashboard
        self.consumer = KafkaConsumer(
            'cybersecurity-stream',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='dashboard-group'
        )
        
        # Data storage
        self.threat_history = deque(maxlen=100)
        self.time_series_data = deque(maxlen=50)
        self.running = True
        
        # Setup matplotlib
        plt.style.use('dark_background')
        self.fig, ((self.ax1, self.ax2), (self.ax3, self.ax4)) = plt.subplots(2, 2, figsize=(16, 10))
        self.fig.suptitle('🛡️ Real-Time Cybersecurity Monitoring Dashboard', fontsize=16, color='white')
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(target=self.consume_data, daemon=True)
        self.consumer_thread.start()
        
        print("📊 Real-Time Dashboard initialized!")
        print("🔍 Monitoring cybersecurity-stream for visualization...")
    
    def consume_data(self):
        """Consume data from Kafka for dashboard"""
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                record = message.value
                
                # Simulate threat prediction (you can integrate actual model here)
                threat_type = record.get('Label', 'Normal')
                confidence = np.random.uniform(0.6, 0.95) if threat_type != 'Normal' else np.random.uniform(0.1, 0.4)
                
                threat_data = {
                    'timestamp': datetime.now(),
                    'type': threat_type,
                    'confidence': confidence,
                    'src_ip': record.get('src_ip', 'unknown')
                }
                
                self.threat_history.append(threat_data)
                
                # Add to time series
                current_time = datetime.now()
                self.time_series_data.append({
                    'time': current_time,
                    'threat_count': 1 if threat_type != 'Normal' else 0
                })
                
        except Exception as e:
            print(f"Dashboard consumer error: {e}")
    
    def update_dashboard(self, frame):
        """Update dashboard plots"""
        if not self.threat_history:
            return
        
        # Clear all axes
        for ax in [self.ax1, self.ax2, self.ax3, self.ax4]:
            ax.clear()
        
        threat_data = list(self.threat_history)
        
        # Plot 1: Threat Distribution (Pie Chart)
        threat_counts = Counter([t['type'] for t in threat_data])
        colors = ['green' if t == 'Normal' else 'red' if t in ['DDoS', 'U2R', 'BOTNET'] 
                 else 'orange' for t in threat_counts.keys()]
        
        if threat_counts:
            wedges, texts, autotexts = self.ax1.pie(
                threat_counts.values(), 
                labels=threat_counts.keys(), 
                autopct='%1.1f%%', 
                colors=colors,
                textprops={'color': 'white'}
            )
            self.ax1.set_title('Threat Distribution', color='white', fontsize=12, fontweight='bold')
        
        # Plot 2: Threat Timeline
        if len(self.time_series_data) > 1:
            times = [t['time'] for t in self.time_series_data]
            threat_counts_timeline = [t['threat_count'] for t in self.time_series_data]
            
            self.ax2.plot(times, threat_counts_timeline, 'cyan', linewidth=2, marker='o', markersize=4)
            self.ax2.set_title('Threats Over Time', color='white', fontsize=12, fontweight='bold')
            self.ax2.tick_params(colors='white')
            self.ax2.set_ylabel('Threat Count', color='white')
            
            # Format x-axis
            self.ax2.tick_params(axis='x', rotation=45)
        
        # Plot 3: Confidence Distribution
        confidences = [t['confidence'] for t in threat_data if t['type'] != 'Normal']
        if confidences:
            self.ax3.hist(confidences, bins=10, color='orange', alpha=0.7, edgecolor='white')
            self.ax3.set_title('Threat Confidence Distribution', color='white', fontsize=12, fontweight='bold')
            self.ax3.tick_params(colors='white')
            self.ax3.set_xlabel('Confidence Score', color='white')
            self.ax3.set_ylabel('Frequency', color='white')
        
        # Plot 4: Top Source IPs
        src_ips = [t['src_ip'] for t in threat_data if t['type'] != 'Normal']
        if src_ips:
            ip_counts = Counter(src_ips)
            top_ips = dict(ip_counts.most_common(5))
            
            if top_ips:
                bars = self.ax4.bar(range(len(top_ips)), list(top_ips.values()), color='red', alpha=0.7)
                self.ax4.set_xticks(range(len(top_ips)))
                self.ax4.set_xticklabels(list(top_ips.keys()), rotation=45, color='white', ha='right')
                self.ax4.set_title('Top Threat Source IPs', color='white', fontsize=12, fontweight='bold')
                self.ax4.tick_params(colors='white')
                self.ax4.set_ylabel('Threat Count', color='white')
                
                # Add value labels on bars
                for bar, value in zip(bars, top_ips.values()):
                    self.ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                                str(value), ha='center', va='bottom', color='white')
        
        # Add timestamp
        self.fig.text(0.02, 0.02, f"Last Updated: {datetime.now().strftime('%H:%M:%S')}", 
                     color='white', fontsize=10)
        
        plt.tight_layout()
    
    def start_dashboard(self):
        """Start the real-time dashboard"""
        print("🚀 Starting real-time dashboard...")
        print("Close the window to stop monitoring")
        
        try:
            # Create animation
            ani = animation.FuncAnimation(
                self.fig, 
                self.update_dashboard, 
                interval=2000,  # Update every 2 seconds
                blit=False
            )
            
            plt.show()
            
        except KeyboardInterrupt:
            print("\n🛑 Dashboard stopped by user")
        finally:
            self.running = False
            self.consumer.close()

if __name__ == "__main__":
    dashboard = RealTimeDashboard()
    dashboard.start_dashboard()

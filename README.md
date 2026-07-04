# Sentinel Stream: Real-Time Intrusion Detection System for SDN Optimized by Fed-Adam 

## Author
- **Name**: Yash Jha
- **Email**: [yashjha024@gmail.com](mailto:yashjha024@gmail.com)
- **Repository**: [https://github.com/yashjha024/Sentinel-Stream.git](https://github.com/yashjha024/Sentinel-Stream.git)

---

## Overview

**Sentinel Stream** is an advanced, privacy-preserving cybersecurity threat detection framework for Software-Defined Networking (SDN) environments. Leveraging **federated learning** with the **FedAdam optimizer** and real-time data streaming via **Apache Kafka**, this project enables distributed, collaborative model training and real-time intrusion detection without centralizing sensitive network data.

The system is designed to detect a wide range of attacks—including DDoS, Probe, DoS, BFA, Web-Attack, BOTNET, and U2R—by combining deep learning, distributed systems, and real-time analytics in a scalable, production-ready architecture.

---

## Table of Contents

- [Author](#author)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Technologies Used](#technologies-used)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Running the System](#running-the-system)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Results](#results)
- [Future Work](#future-work)
- [References](#references)

---

## Features

- **Federated Learning**: Distributed model training across multiple SDN clients without sharing raw data.
- **FedAdam Optimization**: Adaptive, momentum-based optimizer for fast and stable federated convergence.
- **Real-Time Detection**: Kafka-powered streaming for live network traffic simulation and instant threat analysis.
- **Multi-Class Attack Detection**: Identifies DDoS, Probe, DoS, BFA, Web-Attack, BOTNET, U2R, and Normal traffic.
- **Privacy-Preserving**: No centralized data collection; only model updates are shared.
- **Scalable & Modular**: Easily extendable to more clients, new attack types, or additional real-time analytics.
- **Comprehensive Monitoring**: Enhanced alerting, statistics, and real-time dashboard.

---

## System Architecture

![image alt](https://github.com/yashjha024/Sentinel-Stream/blob/main/System_Architecture.png?raw=true)


---

## Technologies Used

- **Python 3.8+**
- **PyTorch** (Deep Learning, Federated Learning)
- **scikit-learn** (Preprocessing, Metrics)
- **pandas, numpy** (Data Handling)
- **matplotlib, seaborn** (Visualization)
- **Apache Kafka** (Real-Time Streaming) [1]
- **kafka-python** (Kafka Producer/Consumer)
- **Jupyter Notebook** (Development & Analysis) [2]
- **VS Code** (Recommended IDE)

---

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Java 8+ (for Kafka)
- Apache Kafka 2.8+ (download from [kafka.apache.org](https://kafka.apache.org/downloads))
- Recommended: 8GB+ RAM, modern CPU, optional CUDA GPU

### Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yashjha024/Sentinel-Stream.git
   cd Sentinel-Stream
   ```

2. **Install Python Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

   
3. **Download and Setup Kafka**
- [Download Kafka](https://kafka.apache.org/downloads)
- Extract and follow [Kafka Quickstart](https://kafka.apache.org/quickstart)

### Running the System

#### 1. **Start Kafka Services**
Open separate terminals and run:
- Zookeeper:  
  `bin/zookeeper-server-start.sh config/zookeeper.properties`
- Kafka Server:  
  `bin/kafka-server-start.sh config/server.properties`
- Create Topic:  
  `bin/kafka-topics.sh --create --topic cybersecurity-stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3`

#### 2. **Train the Federated Model**
- Open `notebook/main_federated.ipynb` in Jupyter and run all cells, or run the training script if provided.

#### 3. **Start Real-Time Simulation**
- **Producer**:  
  `python cybersecurity_producer.py`
- **Consumer/Detector**:  
  `python enhanced_consumer.py`
- **Dashboard (optional)**:  
  `python dashboard.py`

---

## Project Structure

Sentinel-Stream/
│
├── data/ # Raw datasets (metasploitable-2.csv, OVS.csv, Normal_data.csv)
├── output/ # Processed datasets (majority.csv, minority.csv)
├── federated_learning/ # Core FL modules (models.py, client.py, server.py, utils.py)
├── notebook/ # Jupyter notebooks for training and analysis
├── cybersecurity_producer.py # Kafka producer (simulates network traffic)
├── enhanced_consumer.py # Kafka consumer + FL inference + alerting
├── dashboard.py # Real-time visualization dashboard
├── requirements.txt
└── README.md


---

## Usage

- **Federated Learning Training**: Run the notebook to train the model on distributed datasets using FedAdam aggregation.
- **Real-Time Detection**: Launch the producer and consumer to simulate and analyze live network traffic.
- **Monitoring**: Use the dashboard for real-time visualization of detected threats and system performance.

---

## Results

- **Global Test Accuracy**: 97.85%
- **Processing Rate**: ~0.9 messages/second in real-time simulation
- **Threats Detected**: DDoS, Probe, DoS, BFA, Web-Attack, BOTNET, U2R, Normal
- **Privacy**: No raw data sharing; only model parameters exchanged

---

## Future Work

- Integration of advanced federated algorithms (FedProx, FedNova)
- Differential privacy and secure aggregation for enhanced privacy
- Edge deployment for ultra-low latency detection
- Multi-modal data fusion (logs, user behavior, etc.)
- Automated incident response and threat intelligence integration

---

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [PyTorch Documentation](https://pytorch.org/docs/stable/index.html)
- [Federated Learning for Network Attack Detection](https://www.nature.com/articles/s41598-024-70032-2)
- [See full list in the project report]

---

**Sentinel Stream**  
*Advanced, privacy-preserving, real-time SDN threat detection powered by federated learning and adaptive optimization.*


flowchart LR
    %%https://mermaid.live/
    %% PRODUCER LAYER
    subgraph Producer_Layer ["1. PRODUCERS (Host OS JVM)"]
        direction TB
        P["Transaction Producer (Java)"]
        SR[("Confluent Schema Registry\n(Phase 4: Port 8081)")]
        P -.->|"Validate Avro Schema"| SR
    end

    %% BROKER LAYER
    subgraph Broker_Layer ["2. KAFKA BROKER (Docker Container - Port 9092)"]
        direction TB
        
        subgraph T1 ["Topic: raw-payments"]
            direction TB
            P0["Partition 0 (.log file)"]
            P1["Partition 1 (.log file)"]
            Px["Partitions 2-5 (.log files)"]
        end
        
        subgraph T2 ["Topic: fraud-alerts"]
            FA["Partition 0"]
        end
        
        subgraph T3 ["Topic: payments-dlq"]
            DLQ["Partition 0"]
        end
    end

    %% CONSUMER LAYER
    subgraph Consumer_Layer ["3. CONSUMERS & STREAMS (Host OS JVM)"]
        direction TB
        C1["LoggerConsumer\n(Group ID: logger-group-2)"]
        
        subgraph KStreams ["FraudEvaluator (Kafka Streams API)"]
            direction LR
            VA["Velocity Aggregator\n(>3 txns/60s Tumbling Window)"]
            RDB[("RocksDB\n(Local Time-Window State)")]
            VA <-->|"Read/Write Window State"| RDB
        end
    end

    %% DATA FLOW MAPPING
    P ==>|"Phase 1: Async TCP Send\nPhase 2: Hash(userId) routing"| T1
    
    T1 ==>|"Phase 1: Poll / Zero-Copy via OS Page Cache"| C1
    
    T1 ==>|"Phase 3: Stream Ingestion"| KStreams
    
    KStreams ==>|"Phase 3: Fraud Alert Triggered"| T2
    
    KStreams -.->|"Phase 4: Dead Letter Queue (Poison Pill)"| T3
    
    %% STYLING
    classDef storage fill:#f9f2f4,stroke:#333,stroke-width:2px;
    classDef app fill:#e1f5fe,stroke:#0277bd,stroke-width:2px;
    classDef broker fill:#fff3e0,stroke:#e65100,stroke-width:2px;
    
    class P,C1,VA app;
    class T1,T2,T3,P0,P1,Px,FA,DLQ broker;
    class SR,RDB storage;

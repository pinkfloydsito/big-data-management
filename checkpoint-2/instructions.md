# Running the Apache Spark and Kafka Project

## Starting the Environment

Start all containers with a single command:

```bash
docker-compose up -d
```

Verify all services are running:

```bash
docker-compose ps
```

You should see all containers (`pyspark`, `kafka`, `zookeeper`, and `kafka-ui`) in the "Up" state.

## Setting Up Kafka

### Creating a Topic

Create a test topic with appropriate partitioning:

```bash
docker exec kafka kafka-topics --create --topic test-topic \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1
```

Verify the topic was created:

```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

### Producing Test Messages

Send some test messages to the Kafka topic:

```bash
docker exec -it kafka kafka-console-producer --topic test-topic \
  --bootstrap-server kafka:9092
```

Type several test messages (one per line) and press `Ctrl+D` when finished:
```
{"user_id": 1, "action": "login", "timestamp": "2023-06-15T10:30:00Z"}
{"user_id": 2, "action": "purchase", "timestamp": "2023-06-15T10:35:00Z"}
{"user_id": 1, "action": "logout", "timestamp": "2023-06-15T11:30:00Z"}
```

### Monitoring Kafka

Access the Kafka UI dashboard:
- Open [http://localhost:8080](http://localhost:8080) in your browser
- Navigate to the "Topics" section to see your created topic
- Check the "Messages" tab to confirm your test data was properly published

## Running Spark Jobs

### Using the Jupyter Notebook

1. Access Jupyter at [http://localhost:8888](http://localhost:8888)
2. Navigate to your test_kafka_spark.py file or create a new notebook
3. Execute the Spark job to consume messages from Kafka

### Monitoring Spark

When your Spark job is running:
1. Access the Spark UI at [http://localhost:4040](http://localhost:4040)
2. Check the "Jobs" and "Stages" tabs to monitor execution
3. Examine the "Storage" tab to see cached data
4. Use the "Environment" tab to verify loaded packages and configuration

### Check PySpark Console Output

Monitor the Spark job's console output to see processed Kafka messages:

```bash
docker logs -f big_data_project_pyspark
```

Look for output that shows your Kafka messages being processed:

```
+------+------------------------------------------------------------+
|key   |value                                                       |
+------+------------------------------------------------------------+
|null  |{"user_id": 1, "action": "login", "timestamp": "2023-06-15T10:30:00Z"}|
|null  |{"user_id": 2, "action": "purchase", "timestamp": "2023-06-15T10:35:00Z"}|
|null  |{"user_id": 1, "action": "logout", "timestamp": "2023-06-15T11:30:00Z"}|
+------+------------------------------------------------------------+
```

## Testing Integration

### End-to-End Test

1. Run your Spark streaming job in Jupyter
2. Produce new messages using the Kafka console producer
3. Verify that your Spark job processes and displays these new messages

### Testing Different Data Formats

Try sending messages in different formats to test your processing code:
- JSON objects with varied structures
- CSV-like data
- Simple text messages

Example with the console producer:
```
{"event":"signup","details":{"email":"test@example.com"}}
simple plain text message
id,name,value
123,test item,19.99
```

## Troubleshooting

### Common Issues

If your Spark job fails to read from Kafka:

1. Check for connectivity between containers:
   ```bash
   docker exec big_data_project_pyspark ping -c 2 kafka
   ```

2. Verify Kafka topic exists and has messages:
   ```bash
   docker exec kafka kafka-console-consumer --topic test-topic \
     --bootstrap-server kafka:9092 \
     --from-beginning \
     --max-messages 5
   ```

3. Check Spark logs for specific errors:
   ```bash
   docker logs big_data_project_pyspark | grep -i error
   ```

4. Verify Kafka dependencies are properly loaded in Spark:
   ```python
   # Run in your Jupyter notebook
   print(spark.sparkContext._jvm.System.getProperty("java.class.path"))
   ```

# Variables
KAFKA_CONTAINER = kafka
BOOTSTRAP_SERVER = localhost:9092
TOPIC_NAME = raw-taxi-data
NUM_PARTITIONS = 8
REPLICATION_FACTOR = 1

# Topic Management
create-topic:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--create \
		--topic $(TOPIC_NAME) \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--partitions $(NUM_PARTITIONS) \
		--replication-factor $(REPLICATION_FACTOR) \
		--config retention.ms=604800000

delete-topic:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--delete \
		--topic $(TOPIC_NAME) \
		--bootstrap-server $(BOOTSTRAP_SERVER)

list-topics:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--list \
		--bootstrap-server $(BOOTSTRAP_SERVER)

describe-topic:
	docker exec -it $(KAFKA_CONTAINER) kafka-topics \
		--describe \
		--topic $(TOPIC_NAME) \
		--bootstrap-server $(BOOTSTRAP_SERVER)

# Consumer Group Management
list-consumer-groups:
	docker exec -it $(KAFKA_CONTAINER) kafka-consumer-groups \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--list

describe-consumer-group:
	docker exec -it $(KAFKA_CONTAINER) kafka-consumer-groups \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--group message-counter \
		--describe

reset-offsets:
	docker exec -it $(KAFKA_CONTAINER) kafka-consumer-groups \
		--bootstrap-server $(BOOTSTRAP_SERVER) \
		--group message-counter \
		--topic $(TOPIC_NAME) \
		--reset-offsets \
		--to-latest \
		--execute

count-messages:
	docker exec -it kafka kafka-consumer-groups \
  	--bootstrap-server $(BOOTSTRAP_SERVER) \
  	--group message-counter \
  	--topic $(TOPIC_NAME) \
  	--reset-offsets \
  	--to-latest \
  	--execute


# Help command
help:
	@echo "Kafka Management Commands:"
	@echo "  make create-topic          - Create topic with specified partitions and replication"
	@echo "  make delete-topic          - Delete the topic"
	@echo "  make list-topics           - List all topics"
	@echo "  make describe-topic        - Show details about the topic"
	@echo "  make list-consumer-groups  - List all consumer groups"
	@echo "  make describe-consumer-group - Show details about consumer group"
	@echo "  make reset-offsets         - Reset consumer group offsets"
	@echo "  make count-messages         - Counts messages in topic"
	@echo ""
	@echo "Variables (can be overridden):"
	@echo "  KAFKA_CONTAINER = $(KAFKA_CONTAINER)"
	@echo "  BOOTSTRAP_SERVER = $(BOOTSTRAP_SERVER)"
	@echo "  TOPIC_NAME = $(TOPIC_NAME)"
	@echo "  NUM_PARTITIONS = $(NUM_PARTITIONS)"
	@echo "  REPLICATION_FACTOR = $(REPLICATION_FACTOR)"

.PHONY: create-topic delete-topic list-topics describe-topic count-messages \
				list-consumer-groups describe-consumer-group reset-offsets help

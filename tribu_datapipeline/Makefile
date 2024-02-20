AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := "us-east-2"

build_gps_to_celo_map_sync:
	docker build -t rodaapp:gps_to_celo_map_sync -f ./lambda_gps_to_celo_address_map_sync/Dockerfile ../

run_gps_to_celo_map_sync:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:gps_to_celo_map_sync \
		lambda_sync_gps_to_celo_address_map.py

build_extraction:
	docker build -t rodaapp:tribu_extraction -f ./lambda_extraction/Dockerfile ../

run_extraction:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:tribu_extraction \
		lambda_extract_tribu_data.py --dataset-type roda

build_processing:
	docker build -t rodaapp:tribu_processing -f ./lambda_processing/Dockerfile ../

run_processing:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:tribu_processing \
		lambda_process_tribu_data.py --dataset-type guajira

build_blockchain_publisher:
	docker build -t rodaapp:blockchain_publisher -f ./lambda_blockchain_publisher/Dockerfile ../

run_blockchain_publisher:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:blockchain_publisher \
		lambda_blockchain_publish.py --environment staging --date 2023-12-24

docker_login:
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

deploy_extraction_docker_image: docker_login
	docker tag rodaapp:tribu_extraction $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_extraction
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_extraction

deploy_processing_docker_image: docker_login
	docker tag rodaapp:tribu_processing $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_processing
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_processing

deploy_gps_to_celo_map_sync_docker_image: docker_login
	docker tag rodaapp:gps_to_celo_map_sync $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:gps_to_celo_map_sync
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:gps_to_celo_map_sync

deploy_blockchain_publisher_docker_image: docker_login
	docker tag rodaapp:blockchain_publisher $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:blockchain_publisher
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:blockchain_publisher

update_extraction_lambda_function:
	aws lambda update-function-code --function-name extract_tribu_data --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_extraction

update_processing_lambda_function:
	aws lambda update-function-code --function-name process_tribu_data --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_processing

update_gps_to_celo_map_sync_lambda_function:
	aws lambda update-function-code --function-name sync_gps_to_celo_map --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:gps_to_celo_map_sync

update_blockchain_publisher_lambda_function:
	aws lambda update-function-code --function-name publish_to_blockchain --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:blockchain_publisher

backfill_extraction:
ifndef DATASET_TYPE
	$(error DATASET_TYPE is not set)
endif
ifndef DATE
	$(error DATE is not set)
endif
	aws lambda invoke \
		--function-name extract_tribu_data \
		--payload '{"dataset_type":"$(DATASET_TYPE)", "processing_date":"$(DATE)"}' \
		--cli-binary-format raw-in-base64-out \
		/dev/stdout

backfill_processing:
ifndef DATASET_TYPE
	$(error DATASET_TYPE is not set)
endif
ifndef DATE
	$(error DATE is not set)
endif
	aws lambda invoke \
		--function-name process_tribu_data \
		--payload '{"dataset_type":"$(DATASET_TYPE)", "processing_date":"$(DATE)"}' \
		--cli-binary-format raw-in-base64-out \
		/dev/stdout

backfill_gps_to_celo_map_synchronization:
	aws lambda invoke \
		--function-name sync_gps_to_celo_map \
		--payload '{}' \
		--cli-binary-format raw-in-base64-out \
		/dev/stdout

backfill_publish_to_blockchain:
ifndef ENVIRONMENT
	$(error ENVIRONMENT is not set)
endif
ifndef DATE
	$(error DATE is not set)
endif
	aws lambda invoke \
		--function-name publish_to_blockchain \
		--payload '{"environment":"$(ENVIRONMENT)", "processing_date":"$(DATE)"}' \
		--cli-binary-format raw-in-base64-out \
		/dev/stdout

deploy_extraction_lambda_function: build_extraction deploy_extraction_docker_image update_extraction_lambda_function
deploy_processing_lambda_function: build_processing deploy_processing_docker_image update_processing_lambda_function
deploy_gps_to_celo_map_sync_lambda_function: build_gps_to_celo_map_sync deploy_gps_to_celo_map_sync_docker_image update_gps_to_celo_map_sync_lambda_function
deploy_blockchain_publisher_lambda_function: build_blockchain_publisher deploy_blockchain_publisher_docker_image update_blockchain_publisher_lambda_function
deploy_lambda_functions: deploy_extraction_lambda_function deploy_processing_lambda_function deploy_gps_to_celo_map_sync_lambda_function deploy_blockchain_publisher_lambda_function

build_stats_from_blockchain_publisher_logs:
ifndef START_TIMESTAMP
	$(error START_TIMESTAMP is not set)
endif
ifndef END_TIMESTAMP
	$(error END_TIMESTAMP is not set)
endif
	@$(eval START_TIMESTAMP_MS=$(shell date -d "$(START_TIMESTAMP)" +%s%3N))
	@$(eval END_TIMESTAMP_MS=$(shell date -d "$(END_TIMESTAMP)" +%s%3N))
	@aws logs filter-log-events \
		--log-group-name /aws/lambda/publish_to_blockchain \
		--start-time $(START_TIMESTAMP_MS) \
		--end-time $(END_TIMESTAMP_MS) | \
	cut -d\| -f6 | \
	awk '/Parameters:/ { date=$$9 } \
		/Transaction successfully sent/ { success[date]++ } \
		/Publishing route id/ {routes[date]++} \
		/Error publishing route id/ {error[date]++} \
		/Token already minted/ {already_minted[date]++} \
		/insufficient funds for gas/ {insufficient_funds[date]++} \
		/method handler crashed/ {handler_crashed[date]++} \
		/nonce too low/ {low_nonce[date]++} \
		/replacement transaction underpriced/ { underpriced[date]++ } \
		END {for (d in routes) \
			print d, "Routes:", routes[d]+already_minted[d]+0, \
			"Published:", success[d]+already_minted[d]+0, \
			"With error:", error[d]+0, \
			"Already minted:", already_minted[d]+0, \
			"insufficient funds:", insufficient_funds[d]+0, \
			"method handler crashed: ", handler_crashed[d]+0, \
			"nonce too low:", low_nonce[d]+0, \
			"replacement transaction underpriced: ", underpriced[d]+0\
		}' | sort -n 

count_blockchain_routes_staging:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:blockchain_publisher \
		query_blockchain_routes.py
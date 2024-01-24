AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := "us-east-2"

build_gps_to_celo_map_sync:
	docker build -t rodaapp:gps_to_celo_map_sync -f ./Dockerfile_sync .

run_sync_gps_to_celo_map_sync:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd):/var/task \
		-i --entrypoint python rodaapp:gps_to_celo_map_sync \
		lambda_sync_gps_to_celo_address_map.py

build_extraction:
	docker build -t rodaapp:tribu_extraction -f ./lambda_extraction/Dockerfile ../

run_extraction:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd):/var/task \
		-i --entrypoint python rodaapp:tribu_extraction \
		lambda_extract_tribu_data.py --dataset-type roda

build_processing:
	docker build -t rodaapp:tribu_processing -f ./Dockerfile_processing .

run_processing:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd):/var/task \
		-i --entrypoint python rodaapp:tribu_processing \
		lambda_process_tribu_data.py --dataset-type roda

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

update_extraction_lambda_function:
	aws lambda update-function-code --function-name extract_tribu_data --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_extraction

update_processing_lambda_function:
	aws lambda update-function-code --function-name process_tribu_data --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_processing

update_gps_to_celo_map_sync_lambda_function:
	aws lambda update-function-code --function-name sync_gps_to_celo_map --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:gps_to_celo_map_sync

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

deploy_extraction_lambda_function: build_extraction deploy_extraction_docker_image update_extraction_lambda_function
deploy_processing_lambda_function: build_processing deploy_processing_docker_image update_processing_lambda_function
deploy_gps_to_celo_map_sync_lambda_function: build_gps_to_celo_map_sync deploy_gps_to_celo_map_sync_docker_image update_gps_to_celo_map_sync_lambda_function
deploy_lambda_functions: deploy_extraction_lambda_function deploy_processing_lambda_function deploy_gps_to_celo_map_sync_lambda_function
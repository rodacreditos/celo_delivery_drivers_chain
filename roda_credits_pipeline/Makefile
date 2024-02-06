AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := "us-east-2"

docker_login:
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

build_credit_blockchain_publisher:
	docker build -t rodaapp:credit_blockchain_publisher -f ./credit_blockchain_publisher/Dockerfile ../

run_credit_blockchain_publisher:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:credit_blockchain_publisher \
		credit_blockchain_publisher.py --environment staging

deploy_credit_blockchain_publisher_docker_image: docker_login
	docker tag rodaapp:credit_blockchain_publisher $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:credit_blockchain_publisher
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:credit_blockchain_publisher

update_credit_blockchain_publisher_lambda_function:
	aws lambda update-function-code --function-name credit_blockchain_publisher --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:credit_blockchain_publisher

run_payment_blockchain_publisher:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:credit_blockchain_publisher \
		payment_blockchain_publisher.py --environment staging

update_payment_blockchain_publisher_lambda_function:
	aws lambda update-function-code --function-name payment_blockchain_publisher --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:credit_blockchain_publisher

deploy_credit_payments_pipeline: build_credit_blockchain_publisher deploy_credit_blockchain_publisher_docker_image update_credit_blockchain_publisher_lambda_function update_payment_blockchain_publisher_lambda_function

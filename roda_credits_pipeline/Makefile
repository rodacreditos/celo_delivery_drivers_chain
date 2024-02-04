AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := "us-east-2"

build_credit_blockchain_publisher:
	docker build -t rodaapp:credit_blockchain_publisher -f ./credit_blockchain_publisher/Dockerfile ../

run_credit_blockchain_publisher:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-i --entrypoint python rodaapp:credit_blockchain_publisher \
		credit_blockchain_publisher.py --environment staging
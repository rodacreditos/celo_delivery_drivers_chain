AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := "us-east-2"

build_extraction:
	docker build -t rodaapp:tribu_extraction -f ./Dockerfile_extraction .

run_extraction:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd):/var/task \
		-i --entrypoint python rodaapp:tribu_extraction \
		lambda_extract_tribu_data.py --dataset-type roda

docker_login:
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

deploy_docker_image: docker_login
	docker tag rodaapp:tribu_extraction $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_extraction
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:tribu_extraction
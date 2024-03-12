.PHONY: build run login-ecr push-ecr

# Nombre de la imagen Docker
IMAGE_NAME := roda_scoring_builder

# Datos para ECR
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := us-east-2
REPO_NAME := rodaapp

# Construye la imagen
build_scoring_image:
	docker build -t $(REPO_NAME):$(IMAGE_NAME) -f Dockerfile ../

run_scoring_local:
	docker run --rm -it \
		-v ~/.aws:/root/.aws \
		--entrypoint python \
		rodaapp:roda_scoring_builder \
		main.py --environment staging

run_return_airtable_local:
	docker run --rm -it \
			-v ~/.aws:/root/.aws \
			--entrypoint python \
			-e TZ=America/Lima \
			rodaapp:roda_scoring_builder \
			return_airtable.py --environment staging



# Inicia sesión en ecr
login-ecr:
	@aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

# Etiqueta y sube la imagen en Amazon ECR
deploy_scoring_image:
	docker tag rodaapp:roda_scoring_builder $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:roda_scoring_builder
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:roda_scoring_builder


update_scoring_lambda_function:
	aws lambda update-function-code --function-name scoring_model --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:roda_scoring_builder

update_return_airtable_lambda_function:
	aws lambda update-function-code --function-name return_airtable --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:roda_scoring_builder

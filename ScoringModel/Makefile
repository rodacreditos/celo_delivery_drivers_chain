.PHONY: build run login-ecr push-ecr

# Nombre de la imagen Docker
IMAGE_NAME := roda_scoring_builder

# Datos para ECR
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := us-east-2
REPO_NAME := rodaapp

# Construye la imagen
build_scoring_image:
	docker build -t $(REPO_NAME):$(IMAGE_NAME) -f ./Dockerfile ../

run_scoring_image:
	docker run --rm -p 8080:8080 $(IMAGE_NAME)

# Inicia sesión en ecr
login-ecr:
	@aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

# Etiqueta y sube la imagen en Amazon ECR
deploy_scoring_image:
	docker tag rodaapp:roda_scoring_builder $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:roda_scoring_builder
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:roda_scoring_builder



update_scoring_lambda_function:
	aws lambda update-function-code --function-name scoring_model --image-uri $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/rodaapp:roda_scoring_builder

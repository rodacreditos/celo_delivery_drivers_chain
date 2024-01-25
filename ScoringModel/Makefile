.PHONY: build run login-ecr push-ecr

# Nombre de la imagen Docker
IMAGE_NAME := roda_scoring_builder

# Datos para ECR
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query "Account" --output text)
AWS_REGION := us-east-1
REPO_NAME := rodaapp

# Construye la imagen
build:
	docker build -t $(IMAGE_NAME) .

run:
	docker run --rm -p 8080:8080 $(IMAGE_NAME)

# Inicia sesión en ecr
login-ecr:
	@aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

# Etiqueta y sube la imagen en Amazon ECR
push-ecr:
	docker tag $(IMAGE_NAME) $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(REPO_NAME):$(IMAGE_NAME)
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(REPO_NAME):latest

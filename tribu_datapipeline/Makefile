build_extraction:
	docker build -t rodaapp:tribu_extraction -f ./Dockerfile_extraction .

run_extraction:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		rodaapp:tribu_extraction \
		lambda_extract_tribu_data.py --dataset-type roda
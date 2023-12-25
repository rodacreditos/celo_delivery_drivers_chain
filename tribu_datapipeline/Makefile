build_extraction:
	docker build -t rodaapp:tribu_extraction -f ./Dockerfile_extraction .

run_extraction:
	docker run rodaapp:tribu_extraction python lambda_extract_tribu_data.py -o s3://rodaapp-rappidriverchain/guajira_api_bikes.csv

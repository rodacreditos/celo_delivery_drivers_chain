build_dockerimage:
	docker build -t rodaapp:smart_contracts -f ./Dockerfile .

docker_image_bash_console:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd)/credentials:/usr/src/app/credentials \
		-ti --entrypoint bash rodaapp:smart_contracts

deploy_new_smart_contracts_staging: build_dockerimage
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_celo_credentials.yaml ./credentials/roda_celo_credentials.yaml
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd)/credentials:/usr/src/app/credentials \
		-ti --entrypoint bash rodaapp:smart_contracts -c "truffle compile && truffle migrate --network celo_alfajores && truffle run verify RodaRoute --network celo_alfajores"
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_celo_contracts_staging.json \
				s3://rodaapp-rappidriverchain/credentials/roda_celo_contracts_staging.json.backup || true
	aws s3 cp ./credentials/roda_celo_contracts.json s3://rodaapp-rappidriverchain/credentials/roda_celo_contracts_staging.json
	rm ./credentials/roda_celo_credentials.yaml
	rm -f ./credentials/roda_celo_contracts.json

deploy_new_smart_contracts_production: build_dockerimage
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_celo_credentials.yaml ./credentials/roda_celo_credentials.yaml
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd)/credentials:/usr/src/app/credentials \
		-ti --entrypoint bash rodaapp:smart_contracts -c "truffle compile && truffle migrate --network celo_alfajores && truffle run verify RodaRoute --network celo_alfajores"
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_celo_contracts_production.json \
				s3://rodaapp-rappidriverchain/credentials/roda_celo_contracts_production.json.backup || true
	aws s3 cp ./credentials/roda_celo_contracts.json s3://rodaapp-rappidriverchain/credentials/roda_celo_contracts_production.json
	rm ./credentials/roda_celo_credentials.yaml
	rm -f ./credentials/roda_celo_contracts.json


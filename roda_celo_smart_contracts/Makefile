build_dockerimage:
	docker build -t rodaapp:smart_contracts -f ./Dockerfile .

docker_image_bash_console:
	docker run --rm \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd)/credentials:/usr/src/app/credentials \
		-ti --entrypoint bash rodaapp:smart_contracts

deploy_new_roda_route_smart_contract_staging: build_dockerimage
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_celo_credentials.yaml ./credentials/roda_celo_credentials.yaml
	docker run --rm \
		-e SMART_CONTRACT_NAME=RodaRoute \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd)/credentials:/usr/src/app/credentials \
		-ti --entrypoint bash rodaapp:smart_contracts -c "truffle compile && truffle migrate --network celo_alfajores && truffle run verify RodaRoute --network celo_alfajores"
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_routes_contract_staging.json \
				s3://rodaapp-rappidriverchain/credentials/roda_routes_contract_staging.json.backup || true
	aws s3 cp ./credentials/roda_routes_contract.json s3://rodaapp-rappidriverchain/credentials/roda_routes_contract_staging.json
	rm ./credentials/roda_celo_credentials.yaml
	rm -f ./credentials/roda_routes_contract.json

deploy_new_roda_route_smart_contract_production: build_dockerimage
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_celo_credentials.yaml ./credentials/roda_celo_credentials.yaml
	docker run --rm \
		-e SMART_CONTRACT_NAME=RodaRoute \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd)/credentials:/usr/src/app/credentials \
		-ti --entrypoint bash rodaapp:smart_contracts -c "truffle compile && truffle migrate --network celo_alfajores && truffle run verify RodaRoute --network celo_mainnet"
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_routes_contract_production.json \
				s3://rodaapp-rappidriverchain/credentials/roda_routes_contract_production.json.backup || true
	aws s3 cp ./credentials/roda_routes_contract.json s3://rodaapp-rappidriverchain/credentials/roda_routes_contract_production.json
	rm ./credentials/roda_celo_credentials.yaml
	rm -f ./credentials/roda_routes_contract.json

deploy_new_roda_credit_smart_contract_staging: build_dockerimage
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_celo_credentials.yaml ./credentials/roda_celo_credentials.yaml
	docker run --rm \
		-e SMART_CONTRACT_NAME=RodaCreditCOP \
		-v ~/.aws:/root/.aws \
		-v $(shell pwd)/credentials:/usr/src/app/credentials \
		-ti --entrypoint bash rodaapp:smart_contracts -c "truffle compile && truffle migrate --network celo_alfajores && truffle run verify RodaCreditCOP --network celo_alfajores"
	aws s3 cp s3://rodaapp-rappidriverchain/credentials/roda_credits_contract_staging.json \
				s3://rodaapp-rappidriverchain/credentials/roda_credits_contract_staging.json.backup || true
	aws s3 cp ./credentials/roda_credits_contract.json s3://rodaapp-rappidriverchain/credentials/roda_credits_contract_staging.json
	rm ./credentials/roda_celo_credentials.yaml
	rm -f ./credentials/roda_credits_contract.json

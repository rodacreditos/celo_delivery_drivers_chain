deploy_tribu_datapipeline:
	make -C tribu_datapipeline deploy_lambda_functions

upload_guajira_transformation_parameters: transformations_guajira.yaml
	aws s3 cp ./transformations_guajira.yaml s3://rodaapp-rappidriverchain/tribu_metadata/ 

upload_roda_transformation_parameters: transformations_roda.yaml
	aws s3 cp ./transformations_roda.yaml s3://rodaapp-rappidriverchain/tribu_metadata/

upload_roda_tribu_credentials: tribu_roda_credentials.json
	aws s3 cp ./tribu_roda_credentials.json s3://rodaapp-rappidriverchain/credentials/

upload_guajira_tribu_credentials: tribu_guajira_credentials.json
	aws s3 cp ./tribu_guajira_credentials.json s3://rodaapp-rappidriverchain/credentials/

upload_roda_airtable_credentials: roda_airtable_credentials.yaml
	aws s3 cp ./roda_airtable_credentials.yaml s3://rodaapp-rappidriverchain/credentials/

upload_roda_celo_credentials: roda_celo_credentials.yaml
	aws s3 cp ./roda_celo_credentials.yaml s3://rodaapp-rappidriverchain/credentials/

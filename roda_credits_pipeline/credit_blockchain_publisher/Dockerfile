# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Copy requirements file
COPY roda_credits_pipeline/credit_blockchain_publisher/requirements.txt ./

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN pip install --no-cache-dir -r requirements.txt

# Copy function code
COPY tribu_datapipeline/lambda_blockchain_publisher/credit_blockchain_publisher.py ./ 
COPY python_utilities/utils.py ./python_utilities/

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD ["credit_blockchain_publisher.handler"]

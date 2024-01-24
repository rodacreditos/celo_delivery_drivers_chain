# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Copy function code and requirements file
COPY lambda_sync_gps_to_celo_address_map.py ./ 
COPY utils.py ./ 
COPY sync_requirements.txt ./

# Install gcc
RUN yum install -y gcc

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN pip install --no-cache-dir -r sync_requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD ["lambda_sync_gps_to_celo_address_map.handler"]

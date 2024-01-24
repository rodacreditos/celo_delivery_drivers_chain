# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.11

# Copy function code and requirements file
COPY tribu_datapipeline/lambda_extraction/lambda_extract_tribu_data.py ./ 
COPY python_utilities/utils.py ./python_utilities/ 
COPY tribu_datapipeline/lambda_extraction/requirements.txt ./

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN pip install --no-cache-dir -r requirements.txt

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD ["lambda_extract_tribu_data.handler"]

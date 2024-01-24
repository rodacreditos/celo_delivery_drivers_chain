FROM public.ecr.aws/lambda/python:3.11

COPY api_airtable.py ./
COPY main.py ./
COPY requirements.txt ./


# Install dependencies

RUN pip install --no-cache-dir -r requirements.txt

# Set CMD
CMD ["main.handler"]

FROM public.ecr.aws/lambda/python:3.11

COPY ScoringModel/requirements.txt ./

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copia los archivos desde ScoringModel
COPY ScoringModel/api_airtable.py ./
COPY ScoringModel/main.py ./
COPY ScoringModel/social_score.py ./

# Copia utils.py desde python_utilities
COPY python_utilities/utils.py ./python_utilities/

# Set CMD
CMD ["main.handler"]


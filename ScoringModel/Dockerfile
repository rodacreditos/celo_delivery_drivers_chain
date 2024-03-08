FROM public.ecr.aws/lambda/python:3.11

# Copia los archivos desde ScoringModel
COPY ScoringModel/api_airtable.py ./
COPY ScoringModel/main.py ./
COPY ScoringModel/social_score.py ./
COPY ScoringModel/requirements.txt ./

# Copia utils.py desde python_utilities
COPY ./python_utilities/utils.py ./ 

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Set CMD
CMD ["main.handler"]

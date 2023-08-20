FROM python:3.8
COPY requirements.txt requirements.txt

ARG _PROJECT_ID
ENV PROJECT_ID=${_PROJECT_ID}

ENV PORT 8080
ENV HOST 0.0.0.0

RUN pip install -r requirements.txt
COPY . ./
ENTRYPOINT ["python3", "main.py"]
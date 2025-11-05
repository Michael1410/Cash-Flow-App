FROM python:3.12-slim

# install dependencies
WORKDIR /app

# copy the dependencies file to the working directory
COPY /AWS/app/requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local src directory to the working directory
COPY . .

# command to run on container start
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

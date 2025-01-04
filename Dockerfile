# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /code

# Install dependencies for pipenv
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev

# Install pipenv
RUN pip install pipenv
RUN apt-get update && apt-get install -y --no-install-recommends gcc

# Copy the Pipfile and Pipfile.lock to the container
COPY Pipfile Pipfile.lock /code/

# Install the dependencies from Pipfile
RUN pipenv install --system --deploy

# Copy the rest of your application code to the container
COPY app/ /code/app
COPY ./containers/<name_of_your_folder>/initiate.py /code/app

ENTRYPOINT ["python3"]
CMD ["app/main.py"]

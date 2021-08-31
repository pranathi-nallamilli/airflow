# Docker file for File Splitter

FROM python:3.7-slim-buster

# Create base directory
RUN mkdir /app

# Install required packages
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# Copy the required package
COPY ./hxp_ms_pii_file_splitter /app/hxp_ms_pii_file_splitter

# Set working directory
WORKDIR /app

# Set application entrypoint
ENTRYPOINT ["python", "-m", "hxp_ms_pii_file_splitter"]
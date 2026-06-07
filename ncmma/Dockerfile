# Use an official Python runtime as a parent image
FROM python:3.14-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Command to run the application
CMD ["python", "ncmma.py"]

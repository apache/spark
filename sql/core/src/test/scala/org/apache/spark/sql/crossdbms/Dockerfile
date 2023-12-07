# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the `crossdbms` directory contents into the container at /usr/src/app
COPY scala/org/apache/spark/sql/crossdbms/ .

# Copy the SQL input files into the container at /usr/src/app/inputs
COPY resources/sql-tests/inputs ./inputs

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Run cross_dbms_sql_test_runner when the container launches
CMD ["python", "-u" , "./cross_dbms_sql_test_runner.py"]

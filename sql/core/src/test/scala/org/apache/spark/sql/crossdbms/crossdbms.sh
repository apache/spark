#!/bin/bash

# Run the crossdbms app. This starts a postgres container and another container running Python scripts, which connects to the postgres container.
docker-compose --project-directory sql/core/src/test/scala/org/apache/spark/sql/crossdbms up --build --abort-on-container-exit

# Get the container IDs of the container that is generating the golden files.
python_container_id=$(docker ps -qaf 'name=generate_golden_files')
echo "Container id is ${python_container_id}"
echo "Copying generated golden files from ${python_container_id}.."

# Copy the generated golden files from the container to the host machine.
docker cp "${python_container_id}:/usr/src/app/results" sql/core/src/test/resources/sql-tests/

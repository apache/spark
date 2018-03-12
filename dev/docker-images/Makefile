.PHONY: all publish base python r

BASE_IMAGE_NAME = palantirtechnologies/circle-spark-base
PYTHON_IMAGE_NAME = palantirtechnologies/circle-spark-python
R_IMAGE_NAME = palantirtechnologies/circle-spark-r

all: base python r

base: base/Dockerfile
	time docker build base -t ${BASE_IMAGE_NAME}

python: base python/Dockerfile
	time docker build python -t ${PYTHON_IMAGE_NAME}

r: base r/Dockerfile
	time docker build r -t ${R_IMAGE_NAME}

publish: base python r
	docker push ${BASE_IMAGE_NAME}
	docker push ${PYTHON_IMAGE_NAME}
	docker push ${R_IMAGE_NAME}

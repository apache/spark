.PHONY: docker

docker:
	./dev/make-distribution.sh
	cd dist; docker build -t spark:latest -f kubernetes/dockerfiles/spark/Dockerfile .
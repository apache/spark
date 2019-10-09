IMAGE?=radanalyticsio/spark-operator

.PHONY: build
build: package image-build

.PHONY: build-travis
build-travis:
	echo -e "travis_fold:start:mvn\033[33;1mMaven and container build\033[0m"
	$(MAKE) install-lib build
	echo -e "\ntravis_fold:end:mvn\r"

.PHONY: install-parent
install-parent:
	rm -rf ./operator-parent-pom ; git clone --depth=1 --branch master https://github.com/jvm-operators/operator-parent-pom.git && cd operator-parent-pom && MAVEN_OPTS="-Djansi.passthrough=true -Dplexus.logger.type=ansi $(MAVEN_OPTS)" ./mvnw clean install && cd - && rm -rf ./operator-parent-pom

.PHONY: install-lib
install-lib: install-parent
	rm -rf ./abstract-operator ; git clone --depth=1 --branch master https://github.com/jvm-operators/abstract-operator.git && cd abstract-operator && MAVEN_OPTS="-Djansi.passthrough=true -Dplexus.logger.type=ansi $(MAVEN_OPTS)" ./mvnw clean install && cd - && rm -rf ./abstract-operator

.PHONY: package
package:
	MAVEN_OPTS="-Djansi.passthrough=true -Dplexus.logger.type=ansi $(MAVEN_OPTS)" ./mvnw clean package -DskipTests

.PHONY: test
test:
	MAVEN_OPTS="-Djansi.passthrough=true -Dplexus.logger.type=ansi $(MAVEN_OPTS)" ./mvnw clean test

.PHONY: image-build
image-build:
	docker build -t $(IMAGE):ubi -f Dockerfile.ubi .
	docker tag $(IMAGE):ubi $(IMAGE):latest

.PHONY: image-build-alpine
image-build-alpine:
	docker build -t $(IMAGE):alpine -f Dockerfile.alpine .

.PHONY: image-build-all
image-build-all: image-build image-build-alpine

.PHONY: image-publish-alpine
image-publish-alpine: image-build-alpine
	docker tag $(IMAGE):alpine $(IMAGE):alpine-`git rev-parse --short=8 HEAD`
	docker tag $(IMAGE):alpine $(IMAGE):latest-alpine
	docker push $(IMAGE):latest-alpine

.PHONY: image-publish
image-publish: image-build
	docker tag $(IMAGE):ubi $(IMAGE):`git rev-parse --short=8 HEAD`-ubi
	docker tag $(IMAGE):ubi $(IMAGE):latest-ubi
	docker push $(IMAGE):latest

.PHONY: image-publish-all
image-publish-all: build-travis image-build-all image-publish image-publish-alpine

.PHONY: devel
devel: build
	-docker kill `docker ps -q` || true
	oc cluster up ; oc login -u system:admin ; oc project default
	sed 's;quay.io/radanalyticsio/spark-operator:latest-released;radanalyticsio/spark-operator:latest;g' manifest/operator.yaml > manifest/operator-devel.yaml && oc create -f manifest/operator-devel.yaml ; rm manifest/operator-devel.yaml || true
	until [ "true" = "`oc get pod -l app.kubernetes.io/name=spark-operator -o json 2> /dev/null | grep \"\\\"ready\\\": \" | sed -e 's;.*\(true\|false\),;\1;'`" ]; do printf "."; sleep 1; done
	oc logs -f `oc get pods --no-headers -l app.kubernetes.io/name=spark-operator | cut -f1 -d' '`

.PHONY: devel-kubernetes
devel-kubernetes:
	-minikube delete
	minikube start --vm-driver kvm2
	eval `minikube docker-env` && $(MAKE) build
	sed 's;quay.io/radanalyticsio/spark-operator:latest-released;radanalyticsio/spark-operator:latest;g' manifest/operator.yaml > manifest/operator-devel.yaml && kubectl create -f manifest/operator.yaml ; rm manifest/operator-devel.yaml || true
	until [ "true" = "`kubectl get pod -l app.kubernetes.io/name=spark-operator -o json 2> /dev/null | grep \"\\\"ready\\\": \" | sed -e 's;.*\(true\|false\),;\1;'`" ]; do printf "."; sleep 1; done
	kubectl logs -f `kubectl get pods --no-headers -l app.kubernetes.io/name=spark-operator | cut -f1 -d' '`

.PHONY: local-travis-tests
local-travis-tests: build
	-docker kill `docker ps -q` || true
	sed 's;quay.io/radanalyticsio/spark-operator:latest-released;radanalyticsio/spark-operator:latest;g' manifest/operator.yaml > manifest/operator-test.yaml
	-BIN=oc CRD=0 MANIFEST_SUFIX="-test" .travis/.travis.test-oc-and-k8s.sh || true
	-BIN=oc CRD=0 MANIFEST_SUFIX="-test" .travis/.travis.test-restarts.sh || true
	-BIN=oc CRD=0 MANIFEST_SUFIX="-test" .travis/.travis.test-cross-ns.sh || true
	-rm manifest/operator-test.yaml || true

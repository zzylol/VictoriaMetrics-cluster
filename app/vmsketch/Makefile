# All these commands must run from repository root.

run-vmsketch:
	mkdir -p vmsketch-data
	DOCKER_OPTS='-v $(shell pwd)/vmsketch-data:/vmsketch-data' \
	APP_NAME=vmsketch \
	ARGS='-retentionPeriod=12' \
	$(MAKE) run-via-docker

vmsketch:
	APP_NAME=vmsketch $(MAKE) app-local

vmsketch-race:
	APP_NAME=vmsketch RACE=-race $(MAKE) app-local

vmsketch-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker

vmsketch-pure-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-pure

vmsketch-linux-amd64-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-linux-amd64

vmsketch-linux-arm-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-linux-arm

vmsketch-linux-arm64-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-linux-arm64

vmsketch-linux-ppc64le-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-linux-ppc64le

vmsketch-linux-386-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-linux-386

vmsketch-freebsd-amd64-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-freebsd-amd64

vmsketch-openbsd-amd64-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-openbsd-amd64

vmsketch-windows-amd64-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-windows-amd64

vmsketch-darwin-amd64-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-darwin-amd64

vmsketch-darwin-arm64-prod:
	APP_NAME=vmsketch $(MAKE) app-via-docker-darwin-arm64

vmsketch-prod-race:
	APP_NAME=vmsketch RACE=-race $(MAKE) app-via-docker

package-vmsketch:
	APP_NAME=vmsketch $(MAKE) package-via-docker

package-vmsketch-race:
	APP_NAME=vmsketch RACE=-race $(MAKE) package-via-docker

package-vmsketch-amd64:
	APP_NAME=vmsketch $(MAKE) package-via-docker-amd64

package-vmsketch-arm:
	APP_NAME=vmsketch $(MAKE) package-via-docker-arm

package-vmsketch-arm64:
	APP_NAME=vmsketch $(MAKE) package-via-docker-arm64

package-vmsketch-ppc64le:
	APP_NAME=vmsketch $(MAKE) package-via-docker-ppc64le

package-vmsketch-386:
	APP_NAME=vmsketch $(MAKE) package-via-docker-386

publish-vmsketch:
	APP_NAME=vmsketch $(MAKE) publish-via-docker

publish-vmsketch-race:
	APP_NAME=vmsketch RACE=-race $(MAKE) publish-via-docker

vmsketch-linux-amd64:
	APP_NAME=vmsketch CGO_ENABLED=1 GOOS=linux GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmsketch-linux-arm:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=linux GOARCH=arm $(MAKE) app-local-goos-goarch

vmsketch-linux-arm64:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmsketch-linux-ppc64le:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=linux GOARCH=ppc64le $(MAKE) app-local-goos-goarch

vmsketch-linux-s390x:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=linux GOARCH=s390x $(MAKE) app-local-goos-goarch

vmsketch-linux-loong64:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=linux GOARCH=loong64 $(MAKE) app-local-goos-goarch

vmsketch-linux-386:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=linux GOARCH=386 $(MAKE) app-local-goos-goarch

vmsketch-freebsd-amd64:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=freebsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmsketch-openbsd-amd64:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=openbsd GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmsketch-windows-amd64:
	GOARCH=amd64 APP_NAME=vmsketch $(MAKE) app-local-windows-goarch

vmsketch-darwin-amd64:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(MAKE) app-local-goos-goarch

vmsketch-darwin-arm64:
	APP_NAME=vmsketch CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 $(MAKE) app-local-goos-goarch

vmsketch-pure:
	APP_NAME=vmsketch $(MAKE) app-local-pure

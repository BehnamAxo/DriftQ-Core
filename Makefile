SHELL := /bin/sh

.PHONY: ui ui-clean

ui:
	cd ui && npm ci && npm run build

ui-clean:
	rm -rf ui/dist ui/node_modules

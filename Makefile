SHELL := /bin/bash

-include .env
-include Makefile.configure
-include Makefile.specific

BRANCH ?= master
DOMAIN ?= github.com
CODECOV_TOKEN ?=
PART ?= #PATCH #MINOR MAJOR
GH_PACKAGE=$(shell tr '[:upper:]' '[:lower:]' <<< "ghcr.io/$(_USER)/$(_PROJECT):")
SERVICE_NAME=$(shell tr '[:upper:]' '[:lower:]' <<< "$(_PROJECT)-dev")

_BOLD := $(shell tput -T ansi bold)
_COLS := $(shell tput -T ansi cols)
_DEFAULT := $(shell tput -T ansi sgr0)
_ITALICS := $(shell tput -T ansi sitm)
_BLUE := $(shell tput -T ansi setaf 4)
_CYAN := $(shell tput -T ansi setaf 6)
_GREEN := $(shell tput -T ansi setaf 2)
_MAGENTA := $(shell tput -T ansi setaf 5)
_RED := $(shell tput -T ansi setaf 1)
_YELLOW := $(shell tput -T ansi setaf 3)


.PHONY: test-vars
test-vars:
	@echo "Repository:" git@github.com:$(_USER)/$(_PROJECT).git
	@echo "Current working dir: "$(_CURRENT_DIR_NAME)
	@echo "Language:" $(LANGUAGE)
	@echo "Service name:" $(SERVICE_NAME)
	@echo "GH package:" $(GH_PACKAGE)


.PHONY: git
git:  ## reset git, specify new project and git user
	@echo "Configuring git."
	@echo "Project: $(_PROJECT) User: $(_USER)"
	touch Makefile.specific
	git remote rm origin
	git remote add origin git@github.com:$(_USER)/$(_PROJECT).git
	git push --set-upstream origin master


.PHONY: set-project-name
set-project-name: ## (_PROJECT=project _USER=user ) set initial environment
	echo _PROJECT=$(_PROJECT) >> .env && echo _USER=$(_USER) >> .env


.PHONY: bump
bump: venv ## (PART= ) bump the release version - deduced automatically from commit messages unless PART is provided
	. $(_VENV_ACTIVATE) && \
		cz bump --files-only --yes $(if $(PART),--increment=$(PART))


.PHONY: all
all: ## commit and push all changes
	git add .
	git commit -m "feat: auto-commit" --no-verify
	git push
	git status


.PHONY: commit
commit: venv ## make interactive conventional commit
	. $(_VENV_ACTIVATE) && \
		cz commit


.PHONY: help
help: ## display this help message
	$(info Please use $(_BOLD)make $(_DEFAULT)$(_ITALICS)$(_CYAN)target$(_DEFAULT) where \
	$(_ITALICS)$(_CYAN)target$(_DEFAULT) is one of:)
	@grep --no-filename "^[a-zA-Z]" $(MAKEFILE_LIST) | \
		sort | \
		awk -F ":.*?## " 'NF==2 {printf "$(_CYAN)%-20s$(_DEFAULT)%s\n", $$1, $$2}'


.PHONY: changelog
changelog: venv ## (UNRELEASED= current version) update the changelog incrementally.
	@. $(_VENV_ACTIVATE) && \
		cz changelog --incremental --unreleased-version=$(UNRELEASED)
		#make changelog UNRELEASED=$(make get-version)


.PHONY: get-version
get-version: ## output the current version
	@. $(_VENV_ACTIVATE) && \
		cz version --project


.PHONY: docker-download-package
docker-download-package: ##  (VERSION= specifies version) download package from GH package registry
	docker pull $(if $(VERSION), "$(GH_PACKAGE)$(VERSION)", "$(GH_PACKAGE)latest" )
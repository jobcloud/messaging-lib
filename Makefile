.PHONY: clean code-style coverage help test static-analysis update-dependencies
.DEFAULT_GOAL := test

PHPUNIT =  ./vendor/bin/phpunit -c ./phpunit.xml
PHPSTAN  = ./vendor/bin/phpstan analyse --no-progress
PHPCS = ./vendor/bin/phpcs --extensions=php
CONSOLE = ./bin/console
COVCHK = ./vendor/bin/coverage-check

clean:
	rm -rf ./build ./vendor

code-style:
	mkdir -p build/logs/phpcs
	${PHPCS}

coverage:
	${PHPUNIT} && ${COVCHK} build/logs/phpunit/coverage/coverage.xml 100

test:
	${PHPUNIT}

static-analysis:
	mkdir -p build/logs/phpstan
	${PHPSTAN}

update-dependencies:
	composer update

install-dependencies:
	composer install

install-dependencies-lowest:
	composer install --prefer-lowest

help:
	# Usage:
	#   make <target> [OPTION=value]
	#
	# Targets:
	#   clean               Cleans the coverage and the vendor directory
	#   code-style          Check codestyle using phpcs
	#   coverage            Generate code coverage (html, clover)
	#   help                You're looking at it!
	#   test (default)      Run all the tests with phpunit
	#   static-analysis     Run static analysis using phpstan
	#   install-dependencies Run composer install
	#   update-dependencies Run composer update


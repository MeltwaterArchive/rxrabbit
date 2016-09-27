#!/usr/bin/env bash
set -ev
if [ -n "${TRAVIS_TAG}" ]; then
    if [ -z "${BINTRAY_USER}" ]; then
        echo "BINTRAY_USER is unset or set to the empty string"
        exit 1
    fi
    if [ -z "${BINTRAY_KEY}" ]; then
        echo "BINTRAY_KEY is unset or set to the empty string"
        exit 1
    fi
	./gradlew bintray
else
    echo "Will not upload to Bintray."
fi
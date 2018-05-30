#!/bin/sh
set -xe

# runs on code / no need db
bundle exec rake spec

bundle exec ruby-audit check
bundle exec bundle-audit check --update

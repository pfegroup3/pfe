#!/bin/bash

rm -rf dist build src/*.egg-info
python -m build --wheel
databricks bundle validate -t dev --var="warehouse_id=02c55b837849b8af"
databricks bundle deploy   -t dev --var="warehouse_id=02c55b837849b8af"

#!/bin/bash

source $HOME/.bashrc

while getopts "m:" arg; do
    case $arg in
        m) mode=${OPTARG} ;;
    esac
done

app_name=$(cat config.json | \
    python3 -c "import json,sys; print(json.load(sys.stdin)['app_name'])")

if [ -z "$mode" ]; then
    spark_app="importer.py"
else
    spark_app="importer.py --mode $mode"
fi

spark2-submit \
    --master yarn \
    --deploy-mode client \
    --name "$app_name" \
    --jars /opt/cloudera/parcels/JAVA_LIBS/ojdbc8.jar \
    --files config.json \
     $spark_app

#!/bin/bash

set -Eeu

RANGER_URL=$1
RANGER_ADMIN_PASSWORD=$2
SERVICE_TYPE=$3
SERVICE_NAME=$4

#
# Remove existing service instances and servicedef
#
curl -v -i -L -k -u admin:${RANGER_ADMIN_PASSWORD} -H "Content-type: application/json" -X DELETE $RANGER_URL/service/public/v2/api/service/name/${SERVICE_NAME}
curl -v -i -L -k -u admin:${RANGER_ADMIN_PASSWORD} -H "Content-type: application/json" -X DELETE $RANGER_URL/service/public/v2/api/servicedef/name/${SERVICE_TYPE}

echo "Add ${SERVICE_TYPE} Service Def"
curl -v -i -L -k -u admin:${RANGER_ADMIN_PASSWORD} -X POST -H "Accept: application/json" -H "Content-Type: application/json" -d@./ranger-servicedef-${SERVICE_TYPE}.json $RANGER_URL/service/public/v2/api/servicedef

echo "Instantiating ${SERVICE_NAME} Service ..."
curl -v -i -L -k -u admin:${RANGER_ADMIN_PASSWORD} -X POST -H "Accept: application/json" -H "Content-type: application/json" -d@./ranger-service-${SERVICE_NAME}.json $RANGER_URL/service/public/v2/api/service
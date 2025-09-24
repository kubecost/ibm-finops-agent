#!/bin/bash

echo "Starting network troubleshooter!"

read -p "Run troubleshooter with api key? [y/n]: " WITH_APIKEY
echo
read -p "Enter your region: (us/eu/au/me): " REGION
echo

# All regions should be prefixed by - other than us (which uses no suffix)
if [[ -n "$REGION" ]]; then
    if [[ "$REGION" == 'us' ]]; then
        REGION_SUFFIX=""
    else
        REGION_SUFFIX="-$REGION"
    fi
else
    echo "❌ Region not provided. Please enter a valid value."
    exit 1
fi

if [[ "$WITH_APIKEY" == 'n' ]]; then

    FRONTDOOR_HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" https://frontdoor${REGION_SUFFIX}.apptio.com/service/apikeylogin -X POST)
    FRONTDOOR_RESPONSE=$(curl -s https://frontdoor${REGION_SUFFIX}.apptio.com/service/apikeylogin -X POST)

    # Expects 400 response (as apikey is not provided)
    if [[ "$FRONTDOOR_HTTP_STATUS" == "400" ]]; then 
        echo "✅ Login attempt received expected HTTP status."

        if [[ "$FRONTDOOR_RESPONSE" == "login cannot be null (path = null, invalidValue = null)" ]]; then
            echo "✅ Login attempt returned expected reponse. Agent login through frontdoor should be functioning healthily. If problems continue," \
            "please ensure that API keys are up to date and accurate through your frontdoor account."
        else
            echo "❌ Login attempt did not return expected response: $FRONTDOOR_RESPONSE."
        fi
    # Unexpected responses by HTTP Status
    elif [[ "$FRONTDOOR_HTTP_STATUS" == "403" ]]; then 
        echo "❌ Login attempt returned 403 Forbidden status. Please ensure your network is configured so that the agent can reach www.frontdoor${REGION_SUFFIX}.apptio.com" \
        "and AWS to properly upload cluster data. This can often be caused by network proxies or outbound traffic rules which limit potential connections."
    elif [[ "$FRONTDOOR_HTTP_STATUS" == "404" ]]; then 
        echo "❌ Login attempt 404 Not Found status. Please ensure resources in your network are able to connect to external applications on the internet."
    else
        if [[ "$FRONTDOOR_HTTP_STATUS" != "000" ]]; then 
            echo "❌ Login attempt returned $FRONTDOOR_HTTP_STATUS HTTP status."
        fi
        if [[ "$FRONTDOOR_RESPONSE" != "000" ]]; then 
            echo "❌ Login attempt returned response: $FRONTDOOR_RESPONSE."
        fi
        echo "❌ Unexpected response by server. Potential causes could include an improperly configured outbound connection, an issue with certificate" \
        "management, or a temporary resource outage. Please verify there are no issues connecting to frontdoor${REGION_SUFFIX}.apptio.com or AWS."
    fi
elif [[ "$WITH_APIKEY" == 'y' ]]; then
    read -p "Enter your keyAccess: " KEY_ACCESS
    echo
    read -p "Enter your keySecret: " KEY_SECRET
    echo

    OPENTOKEN=$(curl -s -i -X POST \
        -H "Content-Type: application/json" \
        -d '{
            "keyAccess": "'"$KEY_ACCESS"'",
            "keySecret": "'"$KEY_SECRET"'"
        }' https://frontdoor${REGION_SUFFIX}.apptio.com/service/apikeylogin | grep "apptio-opentoken" | awk -F '[=;]' '{print $2}')

    if [ -z "$OPENTOKEN" ]; then 
        echo "❌ Could not fetch opentoken from frontdoor${REGION_SUFFIX}.com with keyAccess and keySecret. Please make sure your credentials are correct" \
        "and not expired. If that's not the issue, verify there are no issues connecting to frontdoor${REGION_SUFFIX}.apptio.com through your network configuration."
        exit 1
    fi
    echo "✅ Successfully fetched opentoken from frontdoor."
    read -p "Enter your cluster UUID: " CLUSTER_UUID
    echo

    if [ -z "$CLUSTER_UUID" ]; then 
        echo "❌ Cluster UUID not provided. Please enter a valid value."
        exit 1
    fi

    read -p "Enter your environment ID: " ENVIRONMENT_ID
    echo

    if [ -z "$ENVIRONMENT_ID" ]; then 
        echo "❌ Environment ID not provided. Please enter a valid value."
        exit 1
    fi

    PRESIGN_HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        -H "Apptio-Environmentid: $ENVIRONMENT_ID" \
        -H "Apptio-Opentoken: $OPENTOKEN" \
        -d '{
            "clusterUID": "'"$CLUSTER_UUID"'",
            "fileName": "'"$CLUSTER_UUID"'_2025-01-01-01-01-01.tgz",
            "agentVersion": "1.0.0",
            "uploadHash": "testingHash"
        }' https://api${REGION_SUFFIX}.cloudability.com/v3/internal/containers/clusters/upload)

    if [ "$PRESIGN_HTTP_STATUS" == "200" ]; then
        echo "✅ Login attempt returned expected reponse. Agent login and generation of presigned S3 URL should be working healthily." \
        "If your problems continue please ensure that your configuration allows PUT requests to S3 buckets."
    else 
        echo "❌ Generation of presigned s3 URL did not return expected code. This could be due to a network traffic exception provided" \
        "for frontdoor${REGION_SUFFIX}.apptio.com but not for AWS. Please check your configuration "
    fi
    exit 0
else
    echo "Incorrect value provided. Please enter 'y' or 'n'."
    exit 1
fi
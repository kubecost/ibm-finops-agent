#!/bin/bash

echo "Starting network troubleshooter!"

read -p "Run troubleshooter with api key? [y/n]: " WITH_APIKEY
echo

if [[ "$WITH_APIKEY" == 'n' ]]; then 
    FRONTDOOR_HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" https://frontdoor.apptio.com/service/apikeylogin -X POST)
    FRONTDOOR_RESPONSE=$(curl -s https://frontdoor.apptio.com/service/apikeylogin -X POST)

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
        echo "❌ Login attempt returned 403 Forbidden status. Please ensure your network is configured so that the agent can reach www.frontdoor.apptio.com" \
        "and AWS to properly upload cluster data. This can often be caused by network proxies or outbound traffic rules which limit potential connections."
        echo "For more information on setting up the agent go to: <TODO: AGENT DOC LINK>"
    elif [[ "$FRONTDOOR_HTTP_STATUS" == "404" ]]; then 
        echo "❌ Login attempt 404 Not Found status. Please ensure resources in your network are able to connect to external applications on the internet."
        echo "For more information on setting up the agent go to: <TODO: AGENT DOC LINK>"
    else
        if [[ "$FRONTDOOR_HTTP_STATUS" != "000" ]]; then 
            echo "❌ Login attempt returned $FRONTDOOR_HTTP_STATUS HTTP status."
        fi
        if [[ "$FRONTDOOR_RESPONSE" != "000" ]]; then 
            echo "❌ Login attempt returned response: $FRONTDOOR_RESPONSE."
        fi
        echo "❌ Unexpected response by server. Potential causes could include an improperly configured outbound connection, an issue with certificate" \
        "management, or a temporary resource outage. Please verify there are no issues connecting to www.frontdoor.apptio.com or AWS."
    fi
elif [[ "$WITH_APIKEY" == 'y' ]]; then 
    read -p "Enter your keyAccess: " keyAccess
    echo
    read -p "Enter your keySecret: " keySecret
    echo

    FRONTDOOR_RESPONSE=$(curl -s -i -X POST \
        -H "Content-Type: application/json" \
        -d '{
            "keyAccess": "'"$keyAccess"'",
            "keySecret": "'"$keySecret"'"
        }' \
        https://frontdoor.apptio.com/service/apikeylogin)

    # TODO Read response and use that for generating a presigned s3 url
else
    echo "Incorrect value provided. Please enter 'y' or 'n'."
    exit 1
fi
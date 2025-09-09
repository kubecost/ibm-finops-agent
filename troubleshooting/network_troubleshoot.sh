#!/bin/bash

echo "Starting network troubleshooter!"

# Check response on apikey login endpoint
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" https://frontdoor.apptio.com/service/apikeylogin -X POST)
RESPONSE=$(curl -s https://frontdoor.apptio.com/service/apikeylogin -X POST)

# Expects 400 response (as apikey is not provided)
if [[ "$HTTP_STATUS" == "400" ]]; then 
    echo "✅ Login attempt received expected HTTP status."

    if [[ "$RESPONSE" == "login cannot be null (path = null, invalidValue = null)" ]]; then
        echo "✅ Login attempt returned expected reponse. Agent login through frontdoor should be functioning healthily. If problems continue," \
        "please ensure that API keys are up to date and accurate through your frontdoor account."
    else
        echo "❌ Login attempt did not return expected response: $RESPONSE."
    fi
# Unexpected responses by HTTP Status
elif [[ "$HTTP_STATUS" == "403" ]]; then 
    echo "❌ Login attempt returned 403 Forbidden status. Please ensure your network is configured so that the agent can reach www.frontdoor.apptio.com" \
    "and AWS to properly upload cluster data. This can often be caused by network proxies or outbound traffic rules which limit potential connections."
    echo "For more information on setting up the agent go to: <TODO: AGENT DOC LINK>"
elif [[ "$HTTP_STATUS" == "404" ]]; then 
    echo "❌ Login attempt 404 Not Found status. Please ensure resources in your network are able to connect to external applications on the internet."
    echo "For more information on setting up the agent go to: <TODO: AGENT DOC LINK>"
else
    if [[ "$HTTP_STATUS" != "000" ]]; then 
        echo "❌ Login attempt returned $HTTP_STATUS HTTP status."
    fi
    if [[ "$RESPONSE" != "000" ]]; then 
        echo "❌ Login attempt returned response: $RESPONSE."
    fi
    echo "❌ Unexpected response by server. Potential causes could include an improperly configured outbound connection, an issue with certificate" \
    "management, or a temporary resource outage. Please verify there are no issues connecting to www.frontdoor.apptio.com or AWS."
fi
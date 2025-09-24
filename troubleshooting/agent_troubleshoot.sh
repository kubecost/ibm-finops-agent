#!/bin/bash

echo "Starting agent troubleshooter!"

# Parameters
# Chart name
if [ -z "$1" ]; then
    echo "Defaulting chart name to: unified-agent"
    CHART_NAME=unified-agent
else
    echo "Chart name set to: $1"
    CHART_NAME=$1
fi

# Namespace
if [ -z "$2" ]; then
    echo "Defaulting namespace to: ibm-finops-agent"
    NAMESPACE=ibm-finops-agent
else
    echo "Namespace set to: $2"
    NAMESPACE=$2
fi



# Check existence of helm chart
if helm status $CHART_NAME -n $NAMESPACE > /dev/null 2>&1; then
    echo "✅ $CHART_NAME chart exists."
else
    echo "❌ Cannot find $CHART_NAME chart."
fi

# Check helm chart status
CHART_STATUS=$(helm status $CHART_NAME -n $NAMESPACE | grep "STATUS:" | awk '{print $2}')
if [ "$CHART_STATUS" == "deployed" ]; then
    echo "✅ $CHART_NAME chart has 'Deployed' status."
else
    echo "❌ $CHART_NAME chart has status: $STATUS."
fi

# Check unified agent pod status
POD_STATUS=$(kubectl get pods -n $NAMESPACE -l app.kubernetes.io/name=finops-agent | grep "unified-agent" | awk '{print $3}')
if [ "$POD_STATUS" == "Running" ]; then
    echo "✅ Finops-agent pod has 'Running' status."
else
    echo "❌ Finops-agent pod has status: $POD_STATUS."
fi

# Check unified agent PVC status
PVC_STATUS=$(kubectl get pvc -n $NAMESPACE | grep "unified-agent" | awk '{print $2}')
if [ "$PVC_STATUS" == "Bound" ]; then
    echo "✅ Finops-agent PVC has 'Bound' status."
else
    echo "❌ Finops-agent PVC has status: $PVC_STATUS."
fi

# Check events in namespace
EVENTS=$(kubectl get events -n $NAMESPACE > /dev/null 2>&1) 
if [ -z "$EVENTS" ]; then
    echo "✅ No events found in $NAMESPACE namespace."
else
    echo "⚠️ Events found in $NAMESPACE namespace:"
    echo $EVENTS
fi



echo "⚠️ Note that the agent uploads normally every 10 minutes, so do verify the logs of your pod after it has been alive for that period of time" \
"to ensure that it is healthily uploading."

exit 0
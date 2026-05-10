echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   TTC Streaming Pipeline — Stopping Demo     ║"
echo "╚══════════════════════════════════════════════╝"
echo ""

# Load environment variables from .env file
if [[ -f .env ]]; then 
  set -a
  source ./.env 
  set +a 
fi

# Assign values from .env file to variables for CloudFormation
ACCOUNT_ID=${AWS_ACCOUNT_ID}
REGION=${AWS_REGION}
STACK_NAME=${STACK_NAME}

# Delete CloudFormation stack
echo "▶ Deleting CloudFormation stack: ${STACK_NAME}"
aws cloudformation delete-stack \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}"

echo "CloudFormation stack deleted."
echo "Remember to delete producer.py from the server if you created a new one"
echo ""
echo "╔══════════════════════════════════════════════╗"
echo "║   TTC Streaming Pipeline — Starting Demo     ║"
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
TEMPLATE="infrastructure/template.yaml" # CloudFormation template


# Confirm AWS identity
echo "▶ AWS identity:"
aws sts get-caller-identity --output table
echo ""

# Confirm region
echo "▶ Deploying to region: ${REGION}"
echo "▶ Account ID: ${ACCOUNT_ID}"
echo "▶ Stack name: ${STACK_NAME}"
echo ""

echo "▶ Deploying infrastructure/template.yaml ..."
aws cloudformation deploy \
  --template-file "${TEMPLATE}" \
  --stack-name    "${STACK_NAME}" \
  --region        "${REGION}" \
  --parameter-overrides AccountId="${ACCOUNT_ID}" \
  --capabilities CAPABILITY_NAMED_IAM

echo ""
echo "✅ Infrastructure ready. Start streaming with:"
echo "   python ingestion/producer.py"
echo ""
echo "💡 Remember to run stop_demo.sh after your session to stop billing."
echo ""
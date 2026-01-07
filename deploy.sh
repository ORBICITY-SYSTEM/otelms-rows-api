#!/bin/bash

# Google Cloud Run Deployment Script
# ====================================

set -e

echo "=== OTELMS Cloud Run Deployment ==="
echo ""

# Configuration
PROJECT_ID="orbicity-otelms"
SERVICE_NAME="otelms-calendar-scraper"
REGION="us-central1"
IMAGE_NAME="otelms-scraper"
REGISTRY="us-central1-docker.pkg.dev"
MEMORY="2Gi"
TIMEOUT="540s"

# Environment variables
OTELMS_USERNAME="tamunamaxaradze@yahoo.com"
OTELMS_PASSWORD="Orbicity1234!"
OTELMS_LOGIN_URL="https://116758.otelms.com/login_c2/"
OTELMS_CALENDAR_URL="https://116758.otelms.com/reservation_c2/calendar"
ROWS_API_KEY="rows-1Gn09f0kCTRULFMfdghHrCX5fGNea1m432hZ9PIBlhaC"
ROWS_SPREADSHEET_ID="6TEX2TmAJXfWwBiRltFBuo"
GCS_BUCKET="otelms-data"

echo "Project ID: $PROJECT_ID"
echo "Service Name: $SERVICE_NAME"
echo "Region: $REGION"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI is not installed"
    exit 1
fi

# Set project
echo "Setting project..."
gcloud config set project $PROJECT_ID

# Get latest version number
LATEST_VERSION=$(gcloud container images list-tags $REGISTRY/$PROJECT_ID/otelms-repo/$IMAGE_NAME --format="get(tags )" --limit=1 | grep -oP 'v\K[0-9]+' | head -1)
NEW_VERSION=$((LATEST_VERSION + 1))
IMAGE_TAG="v$NEW_VERSION"

echo "Building image: $IMAGE_TAG"

# Build and push Docker image
echo ""
echo "Building Docker image..."
gcloud builds submit --tag $REGISTRY/$PROJECT_ID/otelms-repo/$IMAGE_NAME:$IMAGE_TAG

# Deploy to Cloud Run
echo ""
echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
    --image $REGISTRY/$PROJECT_ID/otelms-repo/$IMAGE_NAME:$IMAGE_TAG \
    --region=$REGION \
    --memory=$MEMORY \
    --timeout=$TIMEOUT \
    --set-env-vars="OTELMS_USERNAME=$OTELMS_USERNAME,OTELMS_PASSWORD=$OTELMS_PASSWORD,OTELMS_LOGIN_URL=$OTELMS_LOGIN_URL,OTELMS_CALENDAR_URL=$OTELMS_CALENDAR_URL,ROWS_API_KEY=$ROWS_API_KEY,ROWS_SPREADSHEET_ID=$ROWS_SPREADSHEET_ID,GCS_BUCKET=$GCS_BUCKET" \
    --allow-unauthenticated

# Get service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)")

echo ""
echo "=== DEPLOYMENT SUCCESSFUL ==="
echo ""
echo "Service URL: $SERVICE_URL"
echo "Image: $IMAGE_TAG"
echo ""
echo "Test the service:"
echo "  curl $SERVICE_URL"
echo ""
echo "View logs:"
echo "  gcloud logging read \"resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME\" --limit=50"
echo ""
echo "=== SETUP COMPLETE ==="

#!/bin/bash
source activate dlhub

NAME="DLHub"
FLASKDIR=/home/ubuntu/dlhub_service
SOCKFILE=/home/ubuntu/dlhub_service/dlhub.sock
USER=ubuntu
GROUP=ubuntu
NUM_WORKERS=3
KEY_FILE=/home/ubuntu/dlhub_service/config/key.pem
CERT_FILE=/home/ubuntu/dlhub_service/config/cert.pem

echo "Starting $NAME"

# Create the run directory if it doesn't exist
RUNDIR=$(dirname $SOCKFILE)
test -d $RUNDIR || mkdir -p $RUNDIR

# Start your gunicorn
exec gunicorn run:app -b 0.0.0.0:8080 \
  --name $NAME \
  --workers $NUM_WORKERS \
  --certfile $CERT_FILE \
  --keyfile $KEY_FILE \
  --user=$USER --group=$GROUP \
  --bind=unix:$SOCKFILE \
  --timeout 900

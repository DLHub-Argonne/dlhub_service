#!/bin/bash
#source activate dlhub
source /home/ubuntu/.miniconda3/bin/activate /home/ubuntu/.miniconda3/envs/dlhub
#source activate /home/ubuntu/.miniconda3/envs/dlhub
echo "path is $PATH"
source /home/ubuntu/dlhub.sh

NAME="DLHub"
FLASKDIR=/home/ubuntu/dlhub_service
SOCKFILE=/home/ubuntu/dlhub_service/dlhub.sock
USER=ubuntu
GROUP=ubuntu
NUM_WORKERS=3
KEY_FILE=/home/ubuntu/dlhub_service/config/key.pem
CERT_FILE=/home/ubuntu/dlhub_service/config/cert.pem
ACCESSLOG=/home/ubuntu/dlhub_service/dlhub_access_log
ERRORLOG=/home/ubuntu/dlhub_service/dlhub_error_log
PIDFILE=/home/ubuntu/dlhub_service/dlhub_web_service.pid

echo "Starting $NAME"
if test -f "$PIDFILE"; then
    PID=$(<$PIDFILE)
    if test -d /proc/$PID; then
        echo "DLHub Web Service Process already running"
        exit
    else
        rm $PIDFILE
    fi
fi

#Get env variables
#set -o allexport
#source /home/ubuntu/env.cfg
#set +o allexport

# Create the run directory if it doesn't exist
RUNDIR=$(dirname $SOCKFILE)
test -d $RUNDIR || mkdir -p $RUNDIR

#record PID 
echo "$$" > $PIDFILE

# Start your gunicorn
exec gunicorn run:app -b 0.0.0.0:8080 \
  --name $NAME \
  --workers $NUM_WORKERS \
  --certfile $CERT_FILE \
  --keyfile $KEY_FILE \
  --user=$USER --group=$GROUP \
  --bind=unix:$SOCKFILE \
  --timeout 900 \
  --access-logfile $ACCESSLOG \
  --error-logfile $ERRORLOG \
  --capture-output

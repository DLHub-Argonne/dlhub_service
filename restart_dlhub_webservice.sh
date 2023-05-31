#!/bin/bash
source activate dlhub
source /home/ubuntu/dlhub.sh

PIDFILE=/home/ubuntu/dlhub_service/dlhub_web_service.pid
FLASKDIR=/home/ubuntu/dlhub_service

cd $FLASKDIR

if test -f "$PIDFILE"; then
    PID=$(<$PIDFILE)
    if test -d /proc/$PID; then
        kill $PID
        sleep 10
        /home/ubuntu/dlhub_service/gunicorn_log_to_file.sh >& /dev/null &
    else
        /home/ubuntu/dlhub_service/gunicorn_log_to_file.sh >& /dev/null &
    fi
fi



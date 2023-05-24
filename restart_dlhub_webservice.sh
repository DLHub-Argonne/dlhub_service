#!/bin/bash
PIDFILE=/home/ubuntu/dlhub_service/dlhub_web_service.pid
FLASKDIR=/home/ubuntu/dlhub_service

cd $FLASKDIR

if test -f "$PIDFILE"; then
    PID=$(<$PIDFILE)
    if test -d /proc/$PID; then
        kill $PID
        sleep 10
        ./gunicorn_log_to_file.sh >& /dev/null &
    else
        ./gunicorn_log_to_file.sh >& /dev/null &
    fi
fi



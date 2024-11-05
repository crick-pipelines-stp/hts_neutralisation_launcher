#!/bin/bash

# launch all necessary programs for the neutralisation analysis
# in a tmux server and attach the tmux session

redis-server --daemonize yes

tmux new -s ne -d

tmux rename-window -t ne celery-analysis
tmux send-keys -t ne 'celery -A task worker -Q analysis --concurrency=1 --loglevel=INFO -E -n analysis' C-m
sleep 3

tmux new-window -t ne
tmux rename-window -t ne celery-stitching
tmux send-keys -t ne 'celery -A task worker -Q image_stitch --concurrency=3 --loglevel=INFO -E -n image_stitcher' C-m
sleep 3

tmux new-window -t ne
tmux rename-window -t ne celery-titration
tmux send-keys -t ne 'celery -A task worker -Q titration --concurrency=1 --loglevel=INFO -E -n titration' C-m
sleep 3

tmux new-window -t ne
tmux rename-window -t ne celery-titration-stitching
tmux send-keys -t ne 'celery -A task worker -Q image_stitch_titration --concurrency=3 --loglevel=INFO -E -n image_stitcher_titration' C-m
sleep 3

tmux new-window -t ne
tmux rename-window ne flower
tmux send-keys -t ne 'celery --broker=redis://localhost flower -A task --address=0.0.0.0 --port=5555 --basic_auth=${FLOWER_USERNAME}:${FLOWER_PASSWORD}' C-m

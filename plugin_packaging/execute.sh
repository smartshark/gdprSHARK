#!/bin/sh
NEW_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
mkdir "/dev/shm/$NEW_UUID"

COMMAND="python3.5 ${1}/main.py --db-database ${4} --plugin_path ${1} --db-hostname ${5} --db-port ${6} --project-name ${8} --fields /dev/shm/$NEW_UUID"

if [ ! -z ${2+x} ] && [ ${2} != "None" ]; then
COMMAND="$COMMAND --db-user ${2}"
fi

if [ ! -z ${3+x} ] && [ ${3} != "None" ]; then
COMMAND="$COMMAND --db-password ${3}"
fi

if [ ! -z ${7+x} ] && [ ${7} != "None" ]; then
COMMAND="$COMMAND --db-authentication ${7}"
fi

if [ ! -z ${9+x} ] && [ ${9} != "None" ]; then
COMMAND="$COMMAND --debug ${9}"
fi


$COMMAND

rm -rf "/dev/shm/$NEW_UUID"

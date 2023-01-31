#!/bin/bash

if [ $1 == 'qa' ]; then
    ip='161.35.51.17'
elif [ $1 == 'prod' ]; then
    ip='198.199.81.89'
else
    echo "Please specify 'qa' or 'prod' as a command-line argument."
    exit 2
fi
	
ssh root@$ip "/bin/bash -c 'cd scales-satyrn && sh docker_scripts/refresh_api.sh'"

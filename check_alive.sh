#!/bin/bash
source $HOME/.bash_profile
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ps aux | grep java | grep twitter | grep tuananh &> /dev/null
if [ $? -ne 0 ]; then
    echo "No crawler is running!"
    pushd $DIR &> /dev/null
    make run &
fi

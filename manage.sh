#!/bin/bash

# source 'c:/Users/p1319639/Development/pythonenv/Scripts/activate'

# python manage.py gsp

# Set some default values:
# ALPHA=unset
# BETA=unset
# CHARLIE=unset
# DELTA=unset

help() {
    echo "Options: 
    -h, --help          Options list
        --gsp           GSP reports
        --gsp_sdwan     GSP_SDWAN reports"    
    #exit 2
}

PARSED_ARGUMENTS=$(getopt -a -n report -o h --long help,gsp:,gsp_sdwan: -- "$@")
VALID_ARGUMENTS=$?
if [ "$VALID_ARGUMENTS" != "0" ]; then
    help
fi

echo "PARSED_ARGUMENTS is $PARSED_ARGUMENTS"
eval set -- "$PARSED_ARGUMENTS"
while :; do
    case "$1" in
    -h | --help)
        help
        shift
        ;;
    --gsp)
        CHARLIE="$2"
        shift 2
        ;;
    --gsp_sdwan)
        DELTA="$2"
        shift 2
        ;;
    # -- means the end of the arguments; drop this, and break out of the while loop
    --)
        shift
        break
        ;;
    # If invalid options were passed, then getopt should have reported an error,
    # which we checked as VALID_ARGUMENTS when getopt was called...
    *)
        echo "Unexpected option: $1 - this should not happen."
        help
        ;;
    esac
done

# echo "ALPHA   : $ALPHA"
# echo "BETA    : $BETA "
# echo "CHARLIE : $CHARLIE"
# echo "DELTA   : $DELTA"
# echo "Parameters remaining are: $@"
# echo "$1"
# echo "$2"

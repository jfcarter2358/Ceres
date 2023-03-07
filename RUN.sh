#! /usr/bin/env bash

# REQUIRED ENV VARIABLES
# CERESDB_WHITETAIL_LOGGING_ENABLED   | "true"/"false"
# CERESDB_WHITETAIL_LOGGING_HOST      | string
# CERESDB_WHITETAIL_LOGGING_PORT      | int
# CERESDB_WHITETAIL_LOGGING_APP_NAME  | string
# CERESDB_WHITETAIL_LOGGING_SELF_HOST | string

logging_enabled="${CERESDB_WHITETAIL_LOGGING_ENABLED:-false}"
whitetail_host="${CERESDB_WHITETAIL_LOGGING_HOST:-whitetail}"
whitetail_port="${CERESDB_WHITETAIL_LOGGING_PORT:-9002}"
app_name="${CERESDB_WHITETAIL_LOGGING_APP_NAME:-ceresdb}"
self_host="${CERESDB_WHITETAIL_LOGGING_SELF_HOST:-ceresdb}"


SLEEP_AMOUNT="${CERESDB_SLEEP:-"0"}"
echo "Sleeping for ${SLEEP_AMOUNT} seconds"
sleep "${SLEEP_AMOUNT}"

function run_background {
    nohup bash -c "./ceresdb 2>&1 | tee ceresdb.log" &
}

function run_foreground {
    ./ceresdb
}

function process_json {
    log="${1}"

    timestamp="$(echo "${log}" | jq -r '.time')"
    timestamp="${timestamp%?}.000000"

    message="$(echo "${log}" | jq -r '.msg')"
    message="${message//\"/\'}"
    message="$(echo "${message}" | sed 's/[{}]//g')"

    echo "${message}"

    level="$(echo "${log}" | jq -r '.level')"

    level_uppercase="${level^^}"

    data="{\"@timestamp\":\"${timestamp}\",\"message\":\"${message}\",\"level\":\"${level_uppercase}\",\"appName\":\"${app_name}\",\"fields\":{\"application\":\"${app_name}\",\"severity\":\"${level}\",\"hostname\":\"${self_host}\"}}"

    # Send data
    echo "${data}" | nc "${whitetail_host}" "${whitetail_port}"
}

function process_gin {
    log="${1}"

    log="${log:5}"

    OIFS=$IFS
    IFS=$'|' read -rd '' -a gin_array <<<"$log"
    IFS=$OFS

    timestamp_raw="${gin_array[0]}"
    timestamp_raw="$(echo "${timestamp_raw}" | xargs)"
    timestamp_year="${timestamp_raw:0:4}"
    timestamp_month="${timestamp_raw:5:2}"
    timestamp_day="${timestamp_raw:8:2}"
    timestamp_hour="${timestamp_raw:13:2}"
    timestamp_minute="${timestamp_raw:16:2}"
    timestamp_second="${timestamp_raw:19:2}"

    timestamp="${timestamp_year}-${timestamp_month}-${timestamp_day}T${timestamp_hour}:${timestamp_minute}:${timestamp_second}.000000"

    response_code_raw="${gin_array[1]}"
    response_code="$(echo "${response_code_raw}" | xargs)"

    if [[ "${response_code:0:1}" == "2" ]] || [[ "${response_code:0:1}" == "3" ]]; then
        level="INFO"
    else
        level="ERROR"
    fi

    message="${log//\"/\'}"

    data="{\"@timestamp\":\"${timestamp}\",\"message\":\"${message}\",\"level\":\"${level}\",\"appName\":\"${app_name}\",\"fields\":{\"application\":\"${app_name}\",\"severity\":\"${level}\",\"hostname\":\"${self_host}\"}}"

    # Send data
    echo "${data}" | nc "${whitetail_host}" "${whitetail_port}"
}

function process_logs {
    old_log_length="1"

    sleep 2

    echo "Starting logging function..."

    while true; do
        
        # Check that our log file exists
        if [[ -f "ceresdb.log" ]]; then
        
            # Get the file's current length
            current_log_length="$(cat ceresdb.log | wc -l)"
            ((current_log_length=current_log_length+1))

            # New lines have been added to the log file
            if [[ "${current_log_length}" != "${old_log_length}" ]]; then

                # Grab the new lines and split into lines
                new_lines="$(tail -n +"${old_log_length}" ceresdb.log)"
                OIFS=$IFS
                IFS=$'\n' read -rd '' -a line_array <<<"$new_lines"
                IFS=$OFS

                for log in "${line_array[@]}"; do
                    if [[ "${log:0:1}" == "{" ]]; then
                        process_json "${log}"
                    else
                        process_gin "${log}"
                    fi
                done
                old_log_length="${current_log_length}"
            fi
        fi
    done
}

if [[ "${logging_enabled}" == "true" ]]; then
    run_background

    process_logs
else
    run_foreground
fi

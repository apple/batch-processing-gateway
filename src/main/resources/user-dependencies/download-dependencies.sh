#!/bin/bash

# Copy provided dependencies to desired directory
# Download files in args and extract them if needed.

DEPENDENCIES=$@

if [ -z "$DEPENDENCIES_PATH" ] ; then
    echo "DEPENDENCIES_PATH environment variable is not set."
    exit 1
fi

if [ -z "$NUM_OF_DEPENDENCY_DOWNLOAD_RETRIES" ] ; then
    echo "Dependency download retries (NUM_OF_DEPENDENCY_DOWNLOAD_RETRIES) is not set. Defaulting to 10"
    NUM_OF_DEPENDENCY_DOWNLOAD_RETRIES=10
fi

if [ -z "$DEPENDENCY_DOWNLOAD_READ_TIMEOUT" ] ; then
    echo "Dependency download read timeout (DEPENDENCY_DOWNLOAD_READ_TIMEOUT) is not set. Defaulting to 60 seconds"
    DEPENDENCY_DOWNLOAD_READ_TIMEOUT=60
fi

if [ -z "$CURL_STDERR_FILE_PATH" ] ; then
    CURL_STDERR_FILE_PATH="/dev/curl_stderr"
fi

# Remove the trailing slash if exist
DEPENDENCIES_PATH=${DEPENDENCIES_PATH%/}

for DEPENDENCY in $DEPENDENCIES; do
    echo "Downloading $DEPENDENCY to $DEPENDENCIES_PATH"
    DEPENDENCY_FILE_NAME="$(basename "$DEPENDENCY")"
    STATUS=$(curl --stderr $CURL_STDERR_FILE_PATH --retry "$NUM_OF_DEPENDENCY_DOWNLOAD_RETRIES" --speed-time "$DEPENDENCY_DOWNLOAD_READ_TIMEOUT" --write-out %{http_code} --verbose "$DEPENDENCY" -o "$DEPENDENCIES_PATH/$DEPENDENCY_FILE_NAME")
    if [ "$?" -ne "0" ]; then
        >&2 echo "Error while performing curl to download $DEPENDENCY"
        exit 1
    fi
    if [ "$STATUS" -eq 200 ]; then
        echo "Successfully downloaded $DEPENDENCY to $DEPENDENCIES_PATH"
        if [ "${DEPENDENCY_FILE_NAME: -6}" == "tar.gz" ]; then
            echo "Untarring $DEPENDENCY_FILE_NAME to $DEPENDENCIES_PATH"
            tar -xzf "$DEPENDENCIES_PATH"/"$DEPENDENCY_FILE_NAME" -C "$DEPENDENCIES_PATH"
            echo "Successfully untarred $DEPENDENCY_FILE_NAME to $DEPENDENCIES_PATH"
            echo "Deleting $DEPENDENCY_FILE_NAME from $DEPENDENCIES_PATH"
            rm -f "$DEPENDENCIES_PATH"/"$DEPENDENCY_FILE_NAME"
            echo "Successfully deleted $DEPENDENCY_FILE_NAME from $DEPENDENCIES_PATH"
        fi
    else
        >&2 echo "Cannot download $DEPENDENCY, response ${STATUS}"
        >&2 cat $CURL_STDERR_FILE_PATH
        exit 1
    fi
done

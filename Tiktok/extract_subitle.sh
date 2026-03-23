#!/bin/bash
#!/bin/bash

# Check if the argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <URL>"
    exit 1
fi

# Download the HTML file using curl
curl_output=$(curl -s "https://www.tiktok.com/@dontnkown/video/$1")

# Extracting the JSON object from the HTML file
json=$(echo "$curl_output" | grep -oP '(?<="__DEFAULT_SCOPE__":)[^<]*')

# Removing leading/trailing whitespace and newlines
json=$(echo "$json" | tr -d '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

# Extracting the specific nested object using jq
specific_json=$(echo "$json" | jq '."webapp.video-detail".itemInfo.itemStruct.video.subtitleInfos')


# Loop through each item in the array
for item in $(echo "$specific_json" | jq -r '.[] | @base64'); do
   
    # Decode the base64 encoded JSON string
    decoded_item=$(echo "$item" | base64 -d)
    
    # Extract URL, LanguageCodeName, and Source
    url=$(echo "$decoded_item" | jq -r '.Url')
    language=$(echo "$decoded_item" | jq -r '.LanguageCodeName')
    source=$(echo "$decoded_item" | jq -r '.Source')
    
    # Download the file using curl and save with appropriate name
    filename="${1}.$language.$source"
    curl -o "$filename" "$url"
done

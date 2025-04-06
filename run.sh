#!/bin/env bash

uv sync
source ./.venv/bin/activate

files=()

for vendor in chaldal meenabazar; do
	run_id=$(head -c 8 /dev/random | shasum | head -c 7)
	downloaded_file=raw_files/$run_id.jsonlines

	mkdir -p raw_files/

	start=$(date '+%Y%m%d%H%M%S')
	time scrapy crawl $vendor -o $downloaded_file
	end=$(date '+%Y%m%d%H%M%S')

	final_file=raw_files/${vendor}_${start}_${end}_${run_id}.jsonlines
	mv $downloaded_file $final_file
	files+=($final_file)
done

echo
echo
gzip -v "${files[@]}"

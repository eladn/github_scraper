# GitHub Scrapper
Python script for scrapping GitHub repositories.

## Usage
```
usage: github_scrapper.py [-h] --owners OWNER_NAMES [OWNER_NAMES ...] [--repos REPOSITORY_NAMES [REPOSITORY_NAMES ...]] [--no-popularity-check]
                          --output-dir OUTPUT_DIR_PATH [--user USER] [--token TOKEN] [--main-language MAIN_LANGUAGE]
                          [--main-language-freq MIN_MAIN_LANGUAGE_FREQ] [--extensions FILE_EXTENSIONS [FILE_EXTENSIONS ...]]
                          [--concurrent-api-reqs MAX_NR_CONCURRENT_API_REQUESTS] [--concurrent-downloads MAX_CONCURRENT_DOWNLOADS]
                          [--concurrent-fs-ios MAX_NR_CONCURRENT_FS_IOS] [--attempts MAX_NR_ATTEMPTS]

optional arguments:
  -h, --help            show this help message and exit
  --owners OWNER_NAMES [OWNER_NAMES ...]
  --repos REPOSITORY_NAMES [REPOSITORY_NAMES ...]
  --no-popularity-check
  --output-dir OUTPUT_DIR_PATH
  --user USER
  --token TOKEN
  --main-language MAIN_LANGUAGE
  --main-language-freq MIN_MAIN_LANGUAGE_FREQ
  --extensions FILE_EXTENSIONS [FILE_EXTENSIONS ...]
  --concurrent-api-reqs MAX_NR_CONCURRENT_API_REQUESTS
  --concurrent-downloads MAX_CONCURRENT_DOWNLOADS
  --concurrent-fs-ios MAX_NR_CONCURRENT_FS_IOS
  --attempts MAX_NR_ATTEMPTS
```

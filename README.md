# GitHub Scraper
Python script for scraping GitHub repositories.

## Usage example
```shell
python github_scraper.py \
    --owners apache \
    --output-dir /Users/eladn/Desktop/hdptest \
    --token ghp_XxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXxXx \
    --user myuser \
    --main-language java \
    --extensions java \
    --main-language-freq 0.7
```

## Usage
```
usage: github_scraper.py [-h] --owners OWNER_NAMES [OWNER_NAMES ...] [--repos REPOSITORY_NAMES [REPOSITORY_NAMES ...]] [--no-popularity-check]
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

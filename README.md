# GitHub Scraper
Python script for scraping GitHub repositories. The scraper can scan GitHub profiles or concrete repositories, filter repositories by popularity and/or primary programming language, extract all files of concrete extension into a flattened main directory, and use your account's token for more loosen GitHub's requests ratio limitation. The usage is detailed below. All blocking operations (network requests or file writing) are performed asynchronously to maximize concurrency.

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

## Usage details
```
usage: github_scraper.py [-h] --owners OWNER_NAMES [OWNER_NAMES ...] [--repos REPOSITORY_NAMES [REPOSITORY_NAMES ...]] [--no-popularity-check]
                         --output-dir OUTPUT_DIR_PATH [--user USER] [--token TOKEN] [--main-language MAIN_LANGUAGE]
                         [--main-language-freq MIN_MAIN_LANGUAGE_FREQ] [--extensions FILE_EXTENSIONS [FILE_EXTENSIONS ...]]
                         [--concurrent-api-reqs MAX_NR_CONCURRENT_API_REQUESTS] [--concurrent-downloads MAX_CONCURRENT_DOWNLOADS]
                         [--concurrent-fs-ios MAX_NR_CONCURRENT_FS_IOS] [--attempts MAX_NR_ATTEMPTS]

optional arguments:
  -h, --help            show this help message and exit
  --owners OWNER_NAMES [OWNER_NAMES ...]
    Scan all the repositories of these organizations/private profiles.
  --repos REPOSITORY_NAMES [REPOSITORY_NAMES ...]
    Explicit list of repositories to scrap.
  --no-popularity-check
    If not given, the scraper filters repositories by popularity. The popularity measure is defined in `github_repository_popularity.py`.
  --output-dir OUTPUT_DIR_PATH
  --user USER
    Use your free/premium account for more loosen GitHub's requests ratio limitation.
  --token TOKEN
    Use your free/premium account for more loosen GitHub's requests ratio limitation.
  --main-language MAIN_LANGUAGE
    Keep only repositories of this main language.
  --main-language-freq MIN_MAIN_LANGUAGE_FREQ
    Keep only repositories where the main language has at least such ratio wrt the entire code.
  --extensions FILE_EXTENSIONS [FILE_EXTENSIONS ...]
    If given, the scraper finds only these files in the repository subdirectories and organizes them in the main directory of the scraped repository
  --concurrent-api-reqs MAX_NR_CONCURRENT_API_REQUESTS
    Limit the number of concurrent GitHub API calls.
  --concurrent-downloads MAX_CONCURRENT_DOWNLOADS
    Limit the number of concurrent repositories clones.
  --concurrent-fs-ios MAX_NR_CONCURRENT_FS_IOS
    Limit the number of concurrent file write operations.
  --attempts MAX_NR_ATTEMPTS
    Number of retries for any failable operation before pass.
```

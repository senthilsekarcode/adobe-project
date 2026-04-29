# Adobe Data Engineer Assessment

Version 1 batch pipeline for search attribution reporting.

## Run

```powershell
python main.py
```

The CLI reads `data.txt` by default and writes these generated reports to `outputs/`:

- `YYYY-mm-dd_SearchKeywordPerformance.tab`
- `search_engine_summary.csv`
- `search_keyword_summary.csv`
- `top_keywords.csv`
- `run_summary.json`

Optional arguments:

```powershell
python main.py --input data.txt --output-dir outputs
```

## Deliverable

The required Adobe deliverable is a tab-delimited file named with the UTC run date:

```text
YYYY-mm-dd_SearchKeywordPerformance.tab
```

It contains this exact header row:

```text
Search Engine Domain	Search Keyword	Revenue
```

Rows are sorted by `Revenue` descending. Search engine domain is extracted from the search referrer, for example `google.com` or `search.yahoo.com`.

## Test

```powershell
python -m unittest discover -s tests -p 'test_*.py'
```

## Assumptions

- Input is a tab-separated file with the header columns present in `data.txt`.
- Sessions are keyed by `ip` and `user_agent`; a new session starts after more than 30 minutes of inactivity.
- Attribution uses the first supported external search referrer in a session.
- Supported search engines and keyword parameters are Google `q`, Bing `q`, and Yahoo `p`.
- Keywords are lowercased so equivalent terms with different casing roll up together.
- Purchases are hits where `event_list` contains event token `1`.
- Revenue sums the fourth semicolon-delimited field from each comma-delimited product in `product_list`.
- Generated outputs and Python cache files are intentionally ignored by git.

# AWS Free Tier Deployment Runbook (Adobe Data Engineer Assessment)

## Overview

This document captures how the Python batch pipeline was executed on AWS EC2 Free Tier and how output evidence was generated.

## Environment

- Cloud Provider: AWS
- Service: EC2
- Region: us-east-2 (Ohio)
- OS: Amazon Linux 2023
- Instance Type: t2.micro (Free Tier eligible)
- Execution Date (UTC): 2026-04-29

## Repository

- GitHub Repo: https://github.com/senthilsekarcode/adobe-project
- Branch Used: main
- Input File: `data.txt`

## Setup Commands

```bash
sudo dnf update -y
sudo dnf install -y git python3
python3 --version
git --version
```

## Clone + Checkout
- git clone https://github.com/senthilsekarcode/adobe-project.git
- cd adobe-project
- git checkout main
- ls

## Test Execution
- python3 -m unittest discover -s tests -p "test_*.py" (Test suite passes (OK).)

## Pipeline Execution
- python3 main.py --input data.txt --output-dir outputs

## Output Verification
- ls outputs
- cat outputs/*_SearchKeywordPerformance.tab

## Expected deliverable file format:
File pattern: YYYY-mm-dd_SearchKeywordPerformance.tab
Tab-delimited columns:Search Engine Domain, Search Keyword, Revenue
Sorted by revenue descending.

## Observed Output (Example from run)
Search Engine Domain    Search Keyword    Revenue
google.com              ipod              480.00
bing.com                zune              250.00
search.yahoo.com        cd player         0.00



# Introduction 

- The python library is used to implement the LLM based investment trading system.
- User can use the library to get the historical price data using `Yahoo Finance` (for USA market) API and `Finance Reader Dataset` (for South Korea market) API
- Also, this library contains `flask-server` which are serving the REST API for the `Chat GPT` and `Llama` model.

# Installation

- pre-requisite: 
    - `conda`
    - `git`
    - `.env`
      - contains the environment variables (OPEN AI API KEY, GPT_MODEL_ID, etc..)

- We reconmmend to use `conda` to install the library.
- The library is tested on `Python 3.11` 
```bash
conda create -n llm-investment python=3.11
conda activate llm-investment
pip install -r requirements.txt
pip install -e .
```

# Usage 

- TBD 


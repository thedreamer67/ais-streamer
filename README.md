# AIS Data Streamer and Writer
Python script to stream data from aisstream.io and save to hourly CSV files.

## Setup
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements-min.txt

cp .env.example .env
# Update AISSTREAM_API_KEY in .env
```

## Usage
```bash
# Normal streaming, CTRL + C to stop
./start.sh

# Stream with customised args,  CTRL + C to stop
python stream_ais.py --output_dir data/ais
```

## Test run
```bash
source venv/bin/activate
python stream_ais.py --limit 100  # Test run to stream and write 100 messages
```
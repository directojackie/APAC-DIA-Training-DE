# Generate 1% of target data for quick testing
bash
python scripts/generate_data.py --seed 42 --scale 0.01 --out data_raw

# Check output structure
find data_raw -type f -name "*.csv" | head -10
du -sh data_raw/*
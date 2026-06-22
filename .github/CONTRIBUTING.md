# Contributing

## Running Tests Locally

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Code Style Requirements

All code must pass the following checks before merging:

```bash
# Format code
black producer/ consumer/ stream_processor/ storage/ api/ dashboard/ tests/
isort producer/ consumer/ stream_processor/ storage/ api/ dashboard/ tests/

# Lint
flake8 producer/ consumer/ stream_processor/ storage/ api/ dashboard/ tests/ \
    --max-line-length=100 --ignore=E402,W503

# Type check
mypy producer/ consumer/ stream_processor/ storage/ --ignore-missing-imports
```

## How to Add a New Crypto

1. Add the crypto ID to `CRYPTO_IDS` in `producer/config.py`
2. Add the symbol and color to `CRYPTO_SYMBOLS` and `CRYPTO_COLORS` in `dashboard/app.py`
3. Update the `CRYPTO_IDS` default in `consumer/config_manager.py` (`load_crypto_config`)
4. Update the `VALID_CRYPTO_IDS` env var default in `consumer/data_validator.py`
5. Add tests for the new crypto in `tests/test_crypto_producer.py`

## How to Add a New Pipeline Step

1. Create the processing function in the appropriate module
2. Add a docstring and full type hints
3. Wire it into `consumer/crypto_consumer.py` after the existing steps
4. Add the step name to `PIPELINE_STEPS` in `consumer/pipeline_monitor.py`
5. Add error handling with DLQ via `dead_letter_queue.send_to_dlq`
6. Write unit tests covering success, failure, and edge cases

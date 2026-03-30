# API (C2) for entry-mgmt-automation. Serves GET /runs and GET /entries.
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api.py entry_maps.py mtf_loader.py pip_math.py oanda_client.py scanner_indicators.py scanner_entry_logic.py scanner_state_machine.py scanner_entry_mgmt.py ./

# config.yaml and data/ are typically mounted at run time
ENV PYTHONUNBUFFERED=1
EXPOSE 8000

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]

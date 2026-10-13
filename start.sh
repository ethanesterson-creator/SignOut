#!/bin/sh
# Start command for hosted deployments (Render and similar).
# The camp's secrets arrive either as a secret file (/etc/secrets/secrets.toml)
# or as one environment variable (SECRETS_TOML); either way they are put where
# Streamlit looks for them. Nothing here is specific to any one camp.
set -e

mkdir -p .streamlit
if [ -f /etc/secrets/secrets.toml ]; then
    cp /etc/secrets/secrets.toml .streamlit/secrets.toml
elif [ -n "$SECRETS_TOML" ]; then
    printf '%s' "$SECRETS_TOML" > .streamlit/secrets.toml
else
    echo "No secrets found. Add a secret file named secrets.toml (or a SECRETS_TOML variable) to this service." >&2
    exit 1
fi

exec streamlit run streamlit_app.py \
    --server.port "${PORT:-10000}" \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false

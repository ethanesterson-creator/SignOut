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

# If this service names its camp (CAMP_SLUG), that camp's committed settings are merged
# into the secrets, so changing a setting is a normal push instead of re-pasting secrets.
if [ -n "$CAMP_SLUG" ]; then
    if [ ! -f "deploy_config/$CAMP_SLUG.toml" ]; then
        echo "CAMP_SLUG is '$CAMP_SLUG' but deploy_config/$CAMP_SLUG.toml does not exist." >&2
        exit 1
    fi
    python provision/merge_settings.py merge .streamlit/secrets.toml "deploy_config/$CAMP_SLUG.toml"
fi

# The late-alert worker runs beside the app as its own process, so a late person is
# noticed even if no screen is on a board. It exits with code 3 if the camp has not
# turned it on; any other exit (a crash) is restarted after a few seconds.
(
    while true; do
        python late_worker.py
        [ $? -eq 3 ] && break
        sleep 5
    done
) &


exec streamlit run streamlit_app.py \
    --server.port "${PORT:-10000}" \
    --server.address 0.0.0.0 \
    --server.headless true \
    --browser.gatherUsageStats false

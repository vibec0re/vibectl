#!/bin/bash
# 🔥 VIBEC0RE QUICK DEPLOY SCRIPT 💖
#
# Usage: ./deploy.sh [target-host]
# Example: ./deploy.sh pi@192.168.1.100

set -e

TARGET="${1:-localhost}"
DEPLOY_DIR="/opt/v1bectl"
WEB_DIR="/var/www/v1bectl"

echo "🔥 VIBEC0RE DEPLOYMENT 💖"
echo "Target: $TARGET"
echo ""

# Build if needed
if [ ! -f "target/release/v1bectl_server" ]; then
    echo "📦 Building server..."
    cargo build --release -p v1bectl_server
fi

if [ ! -d "v1bectl_web/dist" ]; then
    echo "📦 Building web UI..."
    cd v1bectl_web && trunk build --release && cd ..
fi

if [ "$TARGET" = "localhost" ]; then
    echo "🚀 Deploying locally..."

    sudo mkdir -p "$DEPLOY_DIR" "$WEB_DIR"
    sudo cp target/release/v1bectl_server "$DEPLOY_DIR/"
    sudo cp -r v1bectl_web/dist/* "$WEB_DIR/"
    sudo cp deploy/v1bectl.service /etc/systemd/system/
    sudo cp deploy/nginx.conf /etc/nginx/sites-available/v1bectl.conf

    sudo ln -sf /etc/nginx/sites-available/v1bectl.conf /etc/nginx/sites-enabled/
    sudo systemctl daemon-reload
    sudo systemctl restart v1bectl nginx

    echo "✅ Deployed locally!"
else
    echo "🚀 Deploying to $TARGET..."

    # Copy files
    ssh "$TARGET" "sudo mkdir -p $DEPLOY_DIR $WEB_DIR"
    scp target/release/v1bectl_server "$TARGET:/tmp/"
    scp -r v1bectl_web/dist/* "$TARGET:/tmp/v1bectl_web/"
    scp deploy/v1bectl.service "$TARGET:/tmp/"
    scp deploy/nginx.conf "$TARGET:/tmp/v1bectl.nginx.conf"

    # Install
    ssh "$TARGET" << 'EOF'
        sudo mv /tmp/v1bectl_server /opt/v1bectl/
        sudo cp -r /tmp/v1bectl_web/* /var/www/v1bectl/
        sudo mv /tmp/v1bectl.service /etc/systemd/system/
        sudo mv /tmp/v1bectl.nginx.conf /etc/nginx/sites-available/v1bectl.conf
        sudo ln -sf /etc/nginx/sites-available/v1bectl.conf /etc/nginx/sites-enabled/
        sudo systemctl daemon-reload
        sudo systemctl restart v1bectl nginx
        echo "✅ Deployed!"
EOF
fi

echo ""
echo "🔥 DEPLOYMENT COMPLETE! 💖"
echo "Server: http://$TARGET:31337"
echo "Web UI: http://$TARGET"

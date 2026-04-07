# GroundHog VPS Deployment

## One-time VPS setup (Hostinger / Ubuntu)

```bash
# 1. Create a dedicated user (optional but recommended)
sudo useradd -r -s /bin/false groundhog

# 2. Clone the repo
sudo git clone https://github.com/<YOUR_USER>/GroudHog.git /opt/groundhog
sudo chown -R groundhog:groundhog /opt/groundhog

# 3. Install the systemd service
sudo cp /opt/groundhog/deploy/groundhog.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable groundhog   # start on boot

# 4. First deploy (sets up venv, deps, data, and starts)
sudo bash /opt/groundhog/deploy/deploy.sh

# 5. Verify it's running
sudo systemctl status groundhog
curl http://localhost:8000/health   # should return {"ok":true}
```

## Reverse proxy (Nginx)

Your portal at `app.voloaltro.tech` should proxy to the backend.  
Add a server block (or location block inside your existing config):

```nginx
server {
    listen 80;
    server_name groundhog.voloaltro.tech;   # or a subpath on app.voloaltro.tech

    location / {
        proxy_pass         http://127.0.0.1:8000;
        proxy_set_header   Host $host;
        proxy_set_header   X-Real-IP $remote_addr;
        proxy_set_header   X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;   # route planning can take a while
    }
}
```

Then:
```bash
sudo nginx -t && sudo systemctl reload nginx
# Add HTTPS with Let's Encrypt:
sudo certbot --nginx -d groundhog.voloaltro.tech
```

## Auto-deploy on git push

### Option A: GitHub webhook (recommended)

1. On GitHub → Settings → Webhooks → Add webhook  
   - Payload URL: `https://groundhog.voloaltro.tech/deploy-hook`  
   - Content type: `application/json`  
   - Secret: a random string (e.g. `openssl rand -hex 20`)  
   - Events: **Just the push event**

2. Set the secret on your VPS:
   ```bash
   echo 'YOUR_SECRET_HERE' | sudo tee /opt/groundhog/.webhook_secret
   sudo chown groundhog:groundhog /opt/groundhog/.webhook_secret
   sudo chmod 600 /opt/groundhog/.webhook_secret
   ```

3. The server already has a `/deploy-hook` endpoint (see below) that
   validates the signature and runs `deploy.sh` in the background.

### Option B: Cron pull (simpler, no webhook needed)

```bash
# Check for updates every 2 minutes
echo "*/2 * * * * root cd /opt/groundhog && git fetch origin && git diff --quiet origin/main || bash /opt/groundhog/deploy/deploy.sh >> /var/log/groundhog-deploy.log 2>&1" \
  | sudo tee /etc/cron.d/groundhog-deploy
```

## Deploying manually

SSH into the VPS and run:
```bash
sudo bash /opt/groundhog/deploy/deploy.sh
```

## Checking logs

```bash
sudo journalctl -u groundhog -f          # live server logs
sudo journalctl -u groundhog --since today  # today's logs
```

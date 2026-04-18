FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . ./

# Keep optional seed data outside mounted volumes so startup can repopulate
# empty volumes (for example obstacles.sqlite built from DOF.CSV).
RUN mkdir -p /app_seed/obstacle_data \
  && if [ -f /app/mvp_backend/obstacle_data/DOF.CSV ]; then cp /app/mvp_backend/obstacle_data/DOF.CSV /app_seed/obstacle_data/DOF.CSV; fi

RUN chmod +x /app/entrypoint.sh

# Persistent data directories (mount as Docker volumes)
VOLUME ["/app/mvp_backend/srtm_cache", "/app/mvp_backend/obstacle_data", "/app/mvp_backend/airspace_data", "/app/faa_nasr"]

EXPOSE 8000

ENTRYPOINT ["/app/entrypoint.sh"]

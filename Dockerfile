FROM node:20-slim AS frontend-builder
WORKDIR /build
COPY boss-frontend/package.json boss-frontend/package-lock.json* ./
RUN npm install
COPY boss-frontend/ ./
RUN npm run build

FROM python:3.10
COPY ./ /indexer/
WORKDIR /indexer/

COPY --from=frontend-builder /build/dist /indexer/boss-frontend/dist

RUN python3 -m venv venv && \
    . ./venv/bin/activate && \
    python3 -m pip install -r requirements.txt

ENTRYPOINT ["/bin/bash", "/indexer/start_server.sh"]
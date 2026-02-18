#!/usr/bin/env bash
uvicorn analytics.api.app.main:app --host 0.0.0.0 --port 

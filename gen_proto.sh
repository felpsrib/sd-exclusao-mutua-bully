#!/usr/bin/env bash
# Gera os stubs gRPC a partir de log_service.proto (equivale a: python gen_proto.py)
cd "$(dirname "$0")" && python gen_proto.py

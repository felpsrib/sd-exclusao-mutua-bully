"""Gera log_service_pb2.py e log_service_pb2_grpc.py a partir de
log_service.proto. Funciona em Linux, macOS e Windows:

    python gen_proto.py
"""
import pathlib
import sys

from grpc_tools import protoc

aqui = pathlib.Path(__file__).resolve().parent
codigo = protoc.main([
    "grpc_tools.protoc",
    f"-I{aqui}",
    f"--python_out={aqui}",
    f"--grpc_python_out={aqui}",
    str(aqui / "log_service.proto"),
])
if codigo == 0:
    print("ok: log_service_pb2.py e log_service_pb2_grpc.py gerados")
sys.exit(codigo)

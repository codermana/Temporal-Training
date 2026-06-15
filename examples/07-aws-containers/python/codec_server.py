"""A PayloadCodec encrypts every payload before it leaves the client/Worker, so the
Temporal server only ever stores ciphertext. A standalone "codec server" runs the
reverse so the Web UI / CLI can decode on demand (with auth) - useful when history
holds S3 URIs or other sensitive references.

The Python port of codec_server.java: the same EncryptionCodec implemented as a
``temporalio.converter.PayloadCodec``, wired into a DataConverter, plus a tiny
remote codec server (``aiohttp`` falls back to ``http.server``) the UI calls.
"""

from typing import Iterable, List

from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.converter import DataConverter, PayloadCodec, default


class EncryptionCodec(PayloadCodec):
    async def encode(self, payloads: Iterable[Payload]) -> List[Payload]:
        return [self._encrypt(p) for p in payloads]  # e.g. AES-GCM

    async def decode(self, payloads: Iterable[Payload]) -> List[Payload]:
        return [self._decrypt(p) for p in payloads]

    def _encrypt(self, payload: Payload) -> Payload:
        raise NotImplementedError  # wrap data in your AES-GCM ciphertext

    def _decrypt(self, payload: Payload) -> Payload:
        raise NotImplementedError


async def codec_client() -> Client:
    # Attach the codec to the default converter so every payload is encoded on the
    # way out and decoded on the way back in.
    converter = DataConverter(
        payload_converter_class=default().payload_converter_class,
        payload_codec=EncryptionCodec(),
    )
    return await Client.connect("127.0.0.1:7233", data_converter=converter)


# --- Remote codec server ----------------------------------------------------
# The Web UI / CLI POST /encode and /decode here so a human can read ciphertext
# history on demand. Same contract as the Java codec server: a list of payloads
# in, the transformed list out, behind your own auth.
def run_codec_server(host: str = "127.0.0.1", port: int = 8081) -> None:
    import json
    from http.server import BaseHTTPRequestHandler, HTTPServer

    from google.protobuf import json_format

    codec = EncryptionCodec()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            import asyncio

            body = self.rfile.read(int(self.headers["Content-Length"]))
            payloads = [
                json_format.Parse(json.dumps(p), Payload())
                for p in json.loads(body)["payloads"]
            ]
            fn = codec.encode if self.path == "/encode" else codec.decode
            out = asyncio.run(fn(payloads))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {"payloads": [json_format.MessageToDict(p) for p in out]}
                ).encode()
            )

    HTTPServer((host, port), Handler).serve_forever()

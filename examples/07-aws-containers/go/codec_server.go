// A PayloadCodec encrypts every payload before it leaves the client/Worker, so the
// Temporal server only ever stores ciphertext. A standalone "codec server" runs the
// reverse so the Web UI / CLI can decode on demand (with auth) - useful when history
// holds S3 URIs or other sensitive references.
//
// The Go port of codec_server.java: the same EncryptionCodec as a
// converter.PayloadCodec, wired into a DataConverter, plus an http handler the
// remote codec server exposes for the UI.
package aws

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
)

// EncryptionCodec encodes (encrypts) on the way out and decodes on the way back.
type EncryptionCodec struct{}

func (EncryptionCodec) Encode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		out[i] = encrypt(p) // e.g. AES-GCM
	}
	return out, nil
}

func (EncryptionCodec) Decode(payloads []*commonpb.Payload) ([]*commonpb.Payload, error) {
	out := make([]*commonpb.Payload, len(payloads))
	for i, p := range payloads {
		out[i] = decrypt(p)
	}
	return out, nil
}

// CodecClientOptions attaches the codec to the default DataConverter so every
// payload is encoded on the way out and decoded on the way back in.
func CodecClientOptions() client.Options {
	dc := converter.NewCodecDataConverter(
		converter.GetDefaultDataConverter(),
		EncryptionCodec{},
	)
	return client.Options{HostPort: "127.0.0.1:7233", DataConverter: dc}
}

// CodecServerHandler is the http.Handler the remote codec server mounts at
// /encode and /decode. The Web UI / CLI POST payloads here so a human can read
// ciphertext history on demand, behind your own auth.
//
//	http.Handle("/encode", CodecServerHandler())
//	http.Handle("/decode", CodecServerHandler())
//	http.ListenAndServe("127.0.0.1:8081", nil)
func CodecServerHandler() converter.PayloadCodec {
	return EncryptionCodec{}
}

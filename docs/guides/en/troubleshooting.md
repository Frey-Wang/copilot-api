# Troubleshooting

[Home](../../../README.md) · [Documentation](README.md) · [简体中文](../zh-CN/troubleshooting.md)

<a id="troubleshooting"></a>

## Troubleshooting

**GitHub Copilot encrypted output decryption failure**

The gateway normalizes agent messages in Guardian JSON-schema requests before forwarding them to GitHub Copilot. Other requests can still receive `Encrypted function output content could not be decrypted or decoded` when the upstream rejects encrypted history.

If this error persists, try a fresh Codex conversation and newly created subagents instead of resuming the failing handoff history. Repeating the same request does not repair that history. Successful reuse of a payload followed by rejection does not establish the upstream encryption key lifetime or scope.

The upstream can send `response.failed` with no error details, followed by an `error` event containing `invalid_request_body` and the decryption message. Codex may stop at the failed event and display only `stream disconnected before completion: response.failed event received`. For HTTP streams, the gateway waits up to one second for the next event and attaches its error details to an otherwise empty failed response.

For GitHub Copilot HTTP streaming, an initial rejection with that exact code and message triggers one retry using plaintext agent messages. This applies both to a failed stream event and to an HTTP 400 JSON error before streaming starts. Both paths share a single retry budget. Only encrypted content blocks in `agent_message` items with non-empty plaintext are omitted on the retry. Plaintext, message authors/recipients, tool results, reasoning, and compaction are retained, and the original request is not mutated. This may omit hidden handoff context that is not represented in the plaintext. Encrypted-only agent messages are not stripped, and failures after output starts or after the retry are forwarded without another attempt. WebSocket and non-streaming requests do not use this recovery.

**Claude models missing from the GitHub Copilot model list**

GitHub Copilot can return a different model list depending on the network location the gateway connects from. If your Copilot account has Claude models enabled but `/v1/models` lists no `claude-*` models and requests for them fail (for example with `model_not_supported`), route the gateway's upstream traffic through your proxy and restart it:

```sh
HTTPS_PROXY=http://127.0.0.1:7890 HTTP_PROXY=http://127.0.0.1:7890 npx @jeffreycao/copilot-api@latest start --proxy-env
```

Replace the address with your proxy. The gateway caches the model list at startup and refreshes it about every 30 minutes; after the list changes, [regenerate the Codex model catalog](codex.md#generate-model_catalogjson).

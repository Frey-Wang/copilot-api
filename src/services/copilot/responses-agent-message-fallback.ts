import type { ServerSentEventMessage } from "fetch-event-stream"

import type {
  ResponsesPayload,
  ResponsesStream,
  ResponseStreamEvent,
} from "~/lib/types/responses"

export async function* createAgentMessageFallbackStream(
  source: ResponsesStream,
  payload: ResponsesPayload,
  retry: (payload: ResponsesPayload) => Promise<ResponsesStream>,
  onRetry: (removedBlocks: number) => void,
): ResponsesStream {
  const fallback = createPlaintextAgentMessageFallback(payload)
  if (!fallback) {
    yield* source
    return
  }

  const iterator = source[Symbol.asyncIterator]()
  const lifecycleChunks: Array<ServerSentEventMessage> = []

  try {
    let result = await iterator.next()
    while (!result.done) {
      const event = parseEvent(result.value.data)
      if (
        (event?.type === "response.created"
          || event?.type === "response.in_progress")
        && lifecycleChunks.length < 2
      ) {
        lifecycleChunks.push(result.value)
        result = await iterator.next()
        continue
      }

      if (isEncryptedHistoryRejection(event)) {
        await iterator.return?.()
        lifecycleChunks.length = 0
        onRetry(fallback.removedBlocks)
        yield* await retry(fallback.payload)
        return
      }

      yield* lifecycleChunks.splice(0)
      yield result.value
      yield* { [Symbol.asyncIterator]: () => iterator }
      return
    }

    yield* lifecycleChunks.splice(0)
  } catch (error) {
    yield* lifecycleChunks.splice(0)
    throw error
  } finally {
    await iterator.return?.()
  }
}

export const createPlaintextAgentMessageFallback = (
  payload: ResponsesPayload,
): { payload: ResponsesPayload; removedBlocks: number } | null => {
  if (!Array.isArray(payload.input)) return null

  let removedBlocks = 0
  const input = payload.input.map((item) => {
    if (
      item.type !== "agent_message"
      || !item.content.some(
        (part) => part.type === "input_text" && part.text.trim(),
      )
    ) {
      return item
    }

    const content = item.content.filter((part) => {
      if (part.type !== "encrypted_content") return true
      removedBlocks += 1
      return false
    })
    return { ...item, content }
  })

  return removedBlocks ?
      { payload: { ...payload, input }, removedBlocks }
    : null
}

const parseEvent = (data: string | undefined): ResponseStreamEvent | null => {
  if (!data || data === "[DONE]") return null
  try {
    return JSON.parse(data) as ResponseStreamEvent
  } catch {
    return null
  }
}

const isEncryptedHistoryRejection = (
  event: ResponseStreamEvent | null,
): boolean => {
  if (!event) return false
  const error =
    event.type === "response.failed" ? event.response?.error
    : event.type === "error" ? (event.error ?? event)
    : null
  return isEncryptedHistoryError(error)
}

export const isEncryptedHistoryError = (error: unknown): boolean =>
  typeof error === "object"
  && error !== null
  && "code" in error
  && error.code === "invalid_request_body"
  && "message" in error
  && error.message
    === "Encrypted function output content could not be decrypted or decoded."

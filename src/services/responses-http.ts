import { events, type ServerSentEventMessage } from "fetch-event-stream"

import type {
  ResponseErrorEvent,
  ResponseFailedEvent,
  ResponsesResult,
  ResponseStreamEvent,
} from "~/lib/types/responses"

const FAILED_EVENT_ERROR_TIMEOUT_MS = 1000

export const createResponsesHttpEventStream = async function* (
  response: Response,
): AsyncGenerator<ServerSentEventMessage, void, unknown> {
  const responseBody = response.body
  if (!responseBody) return

  const reader =
    responseBody.getReader() as ReadableStreamDefaultReader<Uint8Array>
  const readerBackedBody = new ReadableStream<Uint8Array>({
    async pull(controller) {
      const result = await reader.read()
      if (result.done) {
        controller.close()
        return
      }

      controller.enqueue(result.value)
    },
    async cancel(reason) {
      await reader.cancel(reason)
    },
  })

  const iterator = events(new Response(readerBackedBody))[
    Symbol.asyncIterator
  ]()

  try {
    let result = await iterator.next()
    while (!result.done) {
      const chunk = result.value
      const failedEvent = getFailedEventWithoutError(chunk)
      if (!failedEvent) {
        yield chunk
        result = await iterator.next()
        continue
      }

      let timer: ReturnType<typeof setTimeout> | undefined
      let nextResult: IteratorResult<ServerSentEventMessage> | null
      try {
        nextResult = await Promise.race([
          iterator.next(),
          new Promise<null>((resolve) => {
            timer = setTimeout(
              () => resolve(null),
              FAILED_EVENT_ERROR_TIMEOUT_MS,
            )
          }),
        ])
      } catch (error) {
        yield chunk
        throw error
      } finally {
        clearTimeout(timer)
      }

      const error =
        nextResult && !nextResult.done ? getStreamError(nextResult.value) : null
      yield error ?
        {
          ...chunk,
          data: JSON.stringify({
            ...failedEvent,
            response: { ...failedEvent.response, error },
          }),
        }
      : chunk

      if (!nextResult) return
      result = nextResult
    }
  } finally {
    try {
      await reader.cancel()
    } catch {
      // The managed response may already have failed or been cancelled.
    } finally {
      try {
        await iterator.return?.()
      } finally {
        reader.releaseLock()
      }
    }
  }
}

const getFailedEventWithoutError = (
  chunk: ServerSentEventMessage,
): ResponseFailedEvent | null => {
  if (!chunk.data) return null
  try {
    const event = JSON.parse(chunk.data) as ResponseStreamEvent
    return (
        event.type === "response.failed"
          && event.response
          && event.response.error == null
      ) ?
        event
      : null
  } catch {
    return null
  }
}

const getStreamError = (
  chunk: ServerSentEventMessage,
): ResponsesResult["error"] => {
  if (!chunk.data) return null
  try {
    const event = JSON.parse(chunk.data) as ResponseErrorEvent
    if (event.type !== "error") return null
    const error = event.error ?? event
    if (typeof error.message !== "string" || !error.message) return null
    return {
      code: typeof error.code === "string" ? error.code : "upstream_error",
      message: error.message,
    }
  } catch {
    return null
  }
}

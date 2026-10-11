import { describe, expect, test } from "bun:test"

import type { ResponseFailedEvent } from "~/lib/types/responses"

import { createResponsesHttpEventStream } from "~/services/responses-http"

const failedEvent = {
  type: "response.failed",
  sequence_number: 1,
  response: {
    id: "resp-test",
    status: "failed",
    error: null,
    output: [],
  },
}

const upstreamError = {
  type: "error",
  code: "invalid_request_body",
  message:
    "Encrypted function output content could not be decrypted or decoded.",
}

const encodeEvent = (event: { type: string }): string =>
  `event: ${event.type}\nid: upstream-id\ndata: ${JSON.stringify(event)}\n\n`

const parseEvent = (data: string | undefined): unknown =>
  JSON.parse(data ?? "") as unknown

const createResponse = (
  source: Array<{ type: string } & Record<string, unknown>>,
): Response =>
  new Response(source.map(encodeEvent).join(""), {
    headers: { "content-type": "text/event-stream" },
  })

describe("Responses HTTP failure details", () => {
  test("attaches a trailing error before forwarding response.failed", async () => {
    const source = [
      { type: "response.created", sequence_number: 0 },
      failedEvent,
      upstreamError,
    ]
    const chunks = await Array.fromAsync(
      createResponsesHttpEventStream(createResponse(source)),
    )

    expect(chunks.map((chunk) => chunk.event)).toEqual(
      source.map((event) => event.type),
    )
    expect(chunks[1].id).toBe("upstream-id")
    expect(parseEvent(chunks[1].data)).toEqual({
      ...failedEvent,
      response: {
        ...failedEvent.response,
        error: { code: upstreamError.code, message: upstreamError.message },
      },
    })
    expect(parseEvent(chunks[2].data)).toEqual(upstreamError)
  })

  test("accepts nested upstream error details", async () => {
    const chunks = await Array.fromAsync(
      createResponsesHttpEventStream(
        createResponse([
          failedEvent,
          { type: "error", error: { ...upstreamError, type: "api_error" } },
        ]),
      ),
    )

    const event = parseEvent(chunks[0].data) as ResponseFailedEvent
    expect(event.type).toBe("response.failed")
    if (event.type === "response.failed") {
      expect(event.response.error).toEqual({
        code: upstreamError.code,
        message: upstreamError.message,
      })
    }
  })

  test("preserves existing failed response errors", async () => {
    const event = {
      ...failedEvent,
      response: {
        ...failedEvent.response,
        error: { code: "original", message: "Original failure" },
      },
    }
    const chunks = await Array.fromAsync(
      createResponsesHttpEventStream(createResponse([event, upstreamError])),
    )

    expect(parseEvent(chunks[0].data)).toEqual(event)
  })

  test("preserves failed events at EOF and unrelated following events", async () => {
    for (const following of [[], [{ type: "response.completed" }]]) {
      const source = [failedEvent, ...following]
      const chunks = await Array.fromAsync(
        createResponsesHttpEventStream(createResponse(source)),
      )
      expect(chunks.map((chunk) => parseEvent(chunk.data))).toEqual(source)
    }
  })

  test("preserves malformed events and errors without a message", async () => {
    for (const data of ["not-json", "[DONE]", '{"type":"error"}', "null"]) {
      const response = new Response(
        encodeEvent(failedEvent) + `data: ${data}\n\n`,
      )
      const chunks = await Array.fromAsync(
        createResponsesHttpEventStream(response),
      )

      expect(parseEvent(chunks[0].data)).toEqual(failedEvent)
      expect(chunks[1].data).toBe(data)
    }
  })

  test("uses a fallback code when the upstream error omits one", async () => {
    const chunks = await Array.fromAsync(
      createResponsesHttpEventStream(
        createResponse([
          failedEvent,
          { type: "error", message: "Upstream failed" },
        ]),
      ),
    )
    const event = parseEvent(chunks[0].data) as ResponseFailedEvent

    expect(event.response.error).toEqual({
      code: "upstream_error",
      message: "Upstream failed",
    })
  })

  test("handles error details split across body chunks", async () => {
    const encoded = encodeEvent(failedEvent) + encodeEvent(upstreamError)
    let offset = 0
    const response = new Response(
      new ReadableStream<Uint8Array>({
        pull(controller) {
          if (offset >= encoded.length) {
            controller.close()
            return
          }
          controller.enqueue(
            new TextEncoder().encode(encoded.slice(offset, offset + 7)),
          )
          offset += 7
        },
      }),
    )
    const chunks = await Array.fromAsync(
      createResponsesHttpEventStream(response),
    )
    const event = parseEvent(chunks[0].data) as ResponseFailedEvent

    expect(event.response.error?.message).toBe(upstreamError.message)
    expect(response.body?.locked).toBe(false)
  })

  test("forwards a failed event even when reading its trailing error throws", async () => {
    let bodyController: ReadableStreamDefaultController<Uint8Array> | undefined
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          bodyController = controller
          controller.enqueue(new TextEncoder().encode(encodeEvent(failedEvent)))
        },
      }),
    )
    const stream = createResponsesHttpEventStream(response)
    const pending = stream.next()
    const timer = setTimeout(
      () => bodyController?.error(new Error("Read failed")),
      10,
    )
    try {
      const result = await pending
      expect(parseEvent(result.value?.data)).toEqual(failedEvent)
      expect(stream.next()).rejects.toThrow("Read failed")
      await stream.return()
      expect(response.body?.locked).toBe(false)
    } finally {
      clearTimeout(timer)
    }
  })

  test("handles responses without a body", async () => {
    expect(
      await Array.fromAsync(createResponsesHttpEventStream(new Response(null))),
    ).toEqual([])
  })

  test("does not delay ordinary events until another event arrives", async () => {
    let cancelled = false
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(
            new TextEncoder().encode(encodeEvent({ type: "response.created" })),
          )
        },
        cancel() {
          cancelled = true
        },
      }),
    )
    const stream = createResponsesHttpEventStream(response)
    const chunk = await stream.next()

    expect(chunk.done).toBe(false)
    expect(chunk.value?.event).toBe("response.created")
    await stream.return()
    expect(cancelled).toBe(true)
    expect(response.body?.locked).toBe(false)
  })

  test("bounds the wait for missing error details and cancels the reader", async () => {
    let cancelled = false
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(encodeEvent(failedEvent)))
        },
        cancel() {
          cancelled = true
        },
      }),
    )
    const chunks = await Array.fromAsync(
      createResponsesHttpEventStream(response),
    )

    expect(chunks.map((chunk) => parseEvent(chunk.data))).toEqual([failedEvent])
    expect(cancelled).toBe(true)
    expect(response.body?.locked).toBe(false)
  })
})

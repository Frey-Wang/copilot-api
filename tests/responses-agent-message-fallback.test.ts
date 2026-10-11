import { describe, expect, mock, test } from "bun:test"

import type { ResponsesPayload, ResponsesStream } from "~/lib/types/responses"

import { createAgentMessageFallbackStream } from "~/services/copilot/responses-agent-message-fallback"

const payload: ResponsesPayload = {
  model: "gpt-test",
  stream: true,
  input: [
    {
      type: "agent_message",
      author: "/root/reviewer",
      recipient: "/root",
      content: [
        { type: "input_text", text: "Review results" },
        {
          type: "encrypted_content",
          encrypted_content: "encrypted-agent-message",
        },
      ],
    },
    { type: "function_call_output", call_id: "call-1", output: "Tool result" },
    {
      type: "compaction",
      id: "cmp-1",
      encrypted_content: "encrypted-compaction",
    },
    {
      type: "reasoning",
      summary: [],
      encrypted_content: "encrypted-reasoning",
    },
  ],
}

const rejection = {
  type: "response.failed",
  response: {
    error: {
      code: "invalid_request_body",
      message:
        "Encrypted function output content could not be decrypted or decoded.",
    },
  },
}

const streamOf = async function* (
  source: Array<Record<string, unknown>>,
): ResponsesStream {
  for (const event of source) {
    await Promise.resolve()
    yield { data: JSON.stringify(event), event: String(event.type) }
  }
}

describe("Copilot encrypted agent-message recovery", () => {
  test("retries an initial decryption failure with plaintext agent content", async () => {
    const retry = mock(() =>
      Promise.resolve(streamOf([{ type: "response.completed" }])),
    )
    const onRetry = mock((_removedBlocks: number) => {})
    const original = structuredClone(payload)
    const source = streamOf([
      { type: "response.created", response: { id: "failed-response" } },
      { type: "response.in_progress" },
      rejection,
    ])
    const chunks = await Array.fromAsync(
      createAgentMessageFallbackStream(source, payload, retry, onRetry),
    )

    expect(chunks.map((chunk) => chunk.event)).toEqual(["response.completed"])
    expect(retry).toHaveBeenCalledTimes(1)
    expect(retry).toHaveBeenCalledWith({
      ...payload,
      input: [
        {
          type: "agent_message",
          author: "/root/reviewer",
          recipient: "/root",
          content: [{ type: "input_text", text: "Review results" }],
        },
        ...(Array.isArray(payload.input) ? payload.input.slice(1) : []),
      ],
    })
    expect(onRetry).toHaveBeenCalledWith(1)
    expect(payload).toEqual(original)
  })

  test("does not retry a second rejection", async () => {
    const retry = mock(() => Promise.resolve(streamOf([rejection])))
    const chunks = await Array.fromAsync(
      createAgentMessageFallbackStream(
        streamOf([rejection]),
        payload,
        retry,
        () => {},
      ),
    )

    expect(chunks.map((chunk) => chunk.event)).toEqual(["response.failed"])
    expect(retry).toHaveBeenCalledTimes(1)
  })

  test("does not retry after output has started", async () => {
    const source = [
      { type: "response.created" },
      { type: "response.output_item.added" },
      rejection,
    ]
    const retry = mock(() => Promise.resolve(streamOf([])))
    const chunks = await Array.fromAsync(
      createAgentMessageFallbackStream(
        streamOf(source),
        payload,
        retry,
        () => {},
      ),
    )

    expect(chunks.map((chunk) => chunk.event)).toEqual(
      source.map((event) => event.type),
    )
    expect(retry).not.toHaveBeenCalled()
  })

  test("preserves requests with encrypted-only agent content", async () => {
    const encryptedOnly: ResponsesPayload = {
      ...payload,
      input: [
        {
          type: "agent_message",
          author: "/root",
          recipient: "/root/reviewer",
          content: [
            { type: "encrypted_content", encrypted_content: "only-copy" },
          ],
        },
      ],
    }
    const retry = mock(() => Promise.resolve(streamOf([])))
    const chunks = await Array.fromAsync(
      createAgentMessageFallbackStream(
        streamOf([rejection]),
        encryptedOnly,
        retry,
        () => {},
      ),
    )

    expect(chunks.map((chunk) => chunk.event)).toEqual(["response.failed"])
    expect(retry).not.toHaveBeenCalled()
  })

  test("preserves successful streams without retrying", async () => {
    const source = [
      { type: "response.created" },
      { type: "response.in_progress" },
      { type: "response.completed" },
    ]
    const retry = mock(() => Promise.resolve(streamOf([])))
    const chunks = await Array.fromAsync(
      createAgentMessageFallbackStream(
        streamOf(source),
        payload,
        retry,
        () => {},
      ),
    )

    expect(chunks.map((chunk) => chunk.event)).toEqual(
      source.map((event) => event.type),
    )
    expect(retry).not.toHaveBeenCalled()
  })

  test("does not retry unrelated failures or malformed chunks", async () => {
    const retry = mock(() => Promise.resolve(streamOf([])))
    const events = [
      {
        ...rejection,
        response: {
          error: { code: "rate_limit_exceeded", message: "Rate limited" },
        },
      },
      {
        ...rejection,
        response: {
          error: {
            code: "invalid_request_body",
            message: "Other invalid input",
          },
        },
      },
      { ...rejection, response: { error: null } },
      { type: "ping" },
    ]
    for (const event of events) {
      const chunks = await Array.fromAsync(
        createAgentMessageFallbackStream(
          streamOf([event]),
          payload,
          retry,
          () => {},
        ),
      )
      expect(chunks[0]?.data).toBe(JSON.stringify(event))
    }
    for (const data of ["not-json", "[DONE]", "", "null"]) {
      const source = async function* (): ResponsesStream {
        await Promise.resolve()
        yield { data }
      }
      const chunks = await Array.fromAsync(
        createAgentMessageFallbackStream(source(), payload, retry, () => {}),
      )
      expect(chunks[0]?.data).toBe(data)
    }
    expect(retry).not.toHaveBeenCalled()
  })

  test("retries flat and nested initial error events", async () => {
    for (const event of [
      { type: "error", ...rejection.response.error },
      { type: "error", error: rejection.response.error },
    ]) {
      const retry = mock(() =>
        Promise.resolve(streamOf([{ type: "response.completed" }])),
      )
      const chunks = await Array.fromAsync(
        createAgentMessageFallbackStream(
          streamOf([event]),
          payload,
          retry,
          () => {},
        ),
      )
      expect(chunks.map((chunk) => chunk.event)).toEqual(["response.completed"])
      expect(retry).toHaveBeenCalledTimes(1)
    }
  })

  test("closes the rejected source before retrying", async () => {
    let closed = false
    const source = async function* (): ResponsesStream {
      try {
        await Promise.resolve()
        yield { data: JSON.stringify(rejection), event: rejection.type }
        throw new Error("Rejected source must not be read again")
      } finally {
        closed = true
      }
    }
    const retry = mock(() => {
      expect(closed).toBe(true)
      return Promise.resolve(streamOf([{ type: "response.completed" }]))
    })
    await Array.fromAsync(
      createAgentMessageFallbackStream(source(), payload, retry, () => {}),
    )
    expect(closed).toBe(true)
  })

  test("preserves lifecycle events when the source ends or throws", async () => {
    const retry = mock(() => Promise.resolve(streamOf([])))
    const chunks = await Array.fromAsync(
      createAgentMessageFallbackStream(
        streamOf([{ type: "response.created" }]),
        payload,
        retry,
        () => {},
      ),
    )
    expect(chunks.map((chunk) => chunk.event)).toEqual(["response.created"])
    const source = async function* (): ResponsesStream {
      await Promise.resolve()
      yield {
        data: JSON.stringify({ type: "response.created" }),
        event: "response.created",
      }
      throw new Error("Read failed")
    }
    const stream = createAgentMessageFallbackStream(
      source(),
      payload,
      retry,
      () => {},
    )
    expect((await stream.next()).value?.event).toBe("response.created")
    expect(stream.next()).rejects.toThrow("Read failed")
    expect(retry).not.toHaveBeenCalled()
  })

  test("does not duplicate lifecycle events when a later read fails", async () => {
    const source = async function* (): ResponsesStream {
      await Promise.resolve()
      yield { data: '{"type":"response.created"}', event: "response.created" }
      yield {
        data: '{"type":"response.output_item.added"}',
        event: "response.output_item.added",
      }
      throw new Error("Read failed")
    }
    const stream = createAgentMessageFallbackStream(
      source(),
      payload,
      () => Promise.resolve(streamOf([])),
      () => {},
    )
    expect((await stream.next()).value?.event).toBe("response.created")
    expect((await stream.next()).value?.event).toBe(
      "response.output_item.added",
    )
    expect(stream.next()).rejects.toThrow("Read failed")
  })

  test("does not replay the rejected lifecycle when retry dispatch fails", async () => {
    const stream = createAgentMessageFallbackStream(
      streamOf([{ type: "response.created" }, rejection]),
      payload,
      () => Promise.reject(new Error("Retry dispatch failed")),
      () => {},
    )
    await stream.next().then(
      () => {
        throw new Error("Expected retry dispatch to fail")
      },
      (error: unknown) => {
        expect(error).toBeInstanceOf(Error)
        if (error instanceof Error) {
          expect(error.message).toBe("Retry dispatch failed")
        }
      },
    )
  })

  test("does not retry when input is a string or agent plaintext is blank", async () => {
    const retry = mock(() => Promise.resolve(streamOf([])))
    const inputs: Array<ResponsesPayload["input"]> = [
      "Hello",
      [
        {
          type: "agent_message",
          author: "/root",
          recipient: "/root/reviewer",
          content: [
            { type: "input_text", text: "  " },
            { type: "encrypted_content", encrypted_content: "only-copy" },
          ],
        },
      ],
    ]
    for (const input of inputs) {
      const chunks = await Array.fromAsync(
        createAgentMessageFallbackStream(
          streamOf([rejection]),
          { ...payload, input },
          retry,
          () => {},
        ),
      )
      expect(chunks[0]?.event).toBe("response.failed")
    }
    expect(retry).not.toHaveBeenCalled()
  })
})

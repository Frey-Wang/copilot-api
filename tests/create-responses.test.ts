import { afterEach, beforeEach, describe, expect, mock, test } from "bun:test"

import type { ResponsesPayload, ResponsesResult } from "~/lib/types/responses"

import {
  copilotHeaders,
  copilotWebSocketHeaders,
  prepareForCompact,
  prepareInteractionHeaders,
} from "~/lib/api-config"
import { COMPACT_REQUEST } from "~/lib/compact"
import { HTTPError } from "~/lib/error"
import { state } from "~/lib/state"
import { isResponsesStream } from "~/lib/utils"
import {
  buildResponsesWebSocketPoolKey,
  buildResponsesWebSocketPayload,
  buildResponsesWebSocketUrl,
  createResponses,
  prepareResponsesWebSocketRequest,
} from "~/services/copilot/create-responses"

const originalFetch = globalThis.fetch
const originalOauthApp = process.env.COPILOT_API_OAUTH_APP
const originalState = {
  accountType: state.accountType,
  copilotToken: state.copilotToken,
  macMachineId: state.macMachineId,
  vsCodeDeviceId: state.vsCodeDeviceId,
  vsCodeSessionId: state.vsCodeSessionId,
  vsCodeVersion: state.vsCodeVersion,
}

const createResponsesResult = (model: string): ResponsesResult => ({
  created_at: 0,
  error: null,
  id: "resp-test",
  incomplete_details: null,
  instructions: null,
  metadata: null,
  model,
  object: "response",
  output: [],
  output_text: "",
  parallel_tool_calls: false,
  status: "completed",
  temperature: null,
  tool_choice: "auto",
  tools: [],
  top_p: null,
  usage: null,
})

const fetchMock = mock((_url: string | URL | Request, _init?: RequestInit) =>
  Promise.resolve(
    new Response(JSON.stringify(createResponsesResult("gpt-test")), {
      headers: {
        "content-type": "application/json",
      },
      status: 200,
    }),
  ),
)

beforeEach(() => {
  delete process.env.COPILOT_API_OAUTH_APP
  state.accountType = "individual"
  state.copilotToken = "test-token"
  state.macMachineId = "machine-1"
  state.vsCodeDeviceId = "device-1"
  state.vsCodeSessionId = "session-1"
  state.vsCodeVersion = "1.120.0"

  fetchMock.mockClear()
  ;(globalThis as unknown as { fetch: typeof fetch }).fetch =
    fetchMock as unknown as typeof fetch
})

afterEach(() => {
  if (originalOauthApp === undefined) {
    delete process.env.COPILOT_API_OAUTH_APP
  } else {
    process.env.COPILOT_API_OAUTH_APP = originalOauthApp
  }

  state.accountType = originalState.accountType
  state.copilotToken = originalState.copilotToken
  state.macMachineId = originalState.macMachineId
  state.vsCodeDeviceId = originalState.vsCodeDeviceId
  state.vsCodeSessionId = originalState.vsCodeSessionId
  state.vsCodeVersion = originalState.vsCodeVersion
  ;(globalThis as unknown as { fetch: typeof fetch }).fetch = originalFetch
})

describe("createResponses", () => {
  test("keeps HTTP responses requests using x-initiator header", async () => {
    const payload: ResponsesPayload = {
      input: "hello",
      model: "gpt-test",
    }

    await createResponses(payload, {
      initiator: "user",
      requestId: "request-1",
      vision: false,
    })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [, init] = fetchMock.mock.calls[0]
    const requestInit = init as RequestInit & {
      headers: Record<string, string>
    }
    expect(requestInit.headers["x-initiator"]).toBe("user")
    expect(requestInit.headers["X-Request-Id"]).toBeUndefined()

    expect(typeof requestInit.body).toBe("string")
    const body = JSON.parse(requestInit.body as string) as Record<
      string,
      unknown
    >
    expect(body.initiator).toBeUndefined()
    expect(body.type).toBeUndefined()
  })

  test("sets subagent interaction headers for HTTP responses requests", async () => {
    const payload: ResponsesPayload = {
      input: "hello",
      model: "gpt-test",
    }

    await createResponses(payload, {
      initiator: "agent",
      requestId: "request-1",
      sessionId: "interaction-1",
      subagentMarker: {
        agent_id: "agent-1",
        agent_type: "collab_spawn",
        session_id: "sub-session",
      },
      vision: false,
    })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [, init] = fetchMock.mock.calls[0]
    const requestInit = init as RequestInit & {
      headers: Record<string, string>
    }
    expect(requestInit.headers["x-initiator"]).toBe("agent")
    expect(requestInit.headers["x-interaction-id"]).toBe("interaction-1")
    expect(requestInit.headers["x-interaction-type"]).toBe(
      "conversation-subagent",
    )
  })

  test("uses HTTP when websocket transport is requested without stream=true", async () => {
    const payload: ResponsesPayload = {
      input: "hello",
      model: "gpt-test",
      stream: false,
    }

    const response = await createResponses(payload, {
      initiator: "user",
      requestId: "request-1",
      transport: "websocket",
      vision: false,
    })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(response).toEqual(createResponsesResult("gpt-test"))
  })

  test("keeps cache-relevant HTTP payloads deterministic", async () => {
    const payload: ResponsesPayload = {
      input: [
        { role: "user", content: "hello" },
        {
          encrypted_content: "encrypted-reasoning",
          id: "reasoning-1",
          summary: [],
          type: "reasoning",
        },
      ],
      instructions: "stable instructions",
      model: "gpt-test",
      prompt_cache_key: "stable-cache-key",
      stream: false,
    }

    await createResponses(structuredClone(payload), {
      initiator: "user",
      requestId: "request-1",
      vision: false,
    })
    await createResponses(structuredClone(payload), {
      initiator: "user",
      requestId: "request-1",
      vision: false,
    })

    expect(fetchMock.mock.calls[0]?.[1]?.body).toBe(
      fetchMock.mock.calls[1]?.[1]?.body,
    )
    expect(fetchMock.mock.calls[0]?.[1]?.body).toContain(
      '"prompt_cache_key":"stable-cache-key"',
    )
    expect(fetchMock.mock.calls[0]?.[1]?.body).toContain(
      '"encrypted_content":"encrypted-reasoning"',
    )
  })

  test("recovers HTTP encrypted agent-message rejection before forwarding lifecycle events", async () => {
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
              encrypted_content: "bad-encrypted-agent",
            },
          ],
        },
      ],
    }
    const encodeEvents = (events: Array<Record<string, unknown>>): Response =>
      new Response(
        events
          .map(
            (event) =>
              `event: ${String(event.type)}\ndata: ${JSON.stringify(event)}\n\n`,
          )
          .join(""),
        {
          headers: { "content-type": "text/event-stream" },
        },
      )
    const error = {
      code: "invalid_request_body",
      message:
        "Encrypted function output content could not be decrypted or decoded.",
    }
    fetchMock.mockResolvedValueOnce(
      encodeEvents([
        { type: "response.created", response: { id: "first-response" } },
        { type: "response.failed", response: { error: null } },
        { type: "error", ...error },
      ]),
    )
    fetchMock.mockResolvedValueOnce(
      encodeEvents([
        { type: "response.created", response: { id: "retry-response" } },
        {
          type: "response.completed",
          response: createResponsesResult("gpt-test"),
        },
      ]),
    )

    const response = await createResponses(payload, {
      initiator: "agent",
      requestId: "request-1",
      sessionId: "interaction-1",
      vision: false,
    })
    expect(isResponsesStream(response)).toBe(true)
    if (!isResponsesStream(response)) throw new Error("Expected stream")
    const chunks = await Array.fromAsync(response)

    expect(chunks.map((chunk) => chunk.event)).toEqual([
      "response.created",
      "response.completed",
    ])
    expect(chunks[0].data).toContain("retry-response")
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock.mock.calls[0]?.[1]?.body).toContain("bad-encrypted-agent")
    expect(fetchMock.mock.calls[1]?.[1]?.body).not.toContain(
      "bad-encrypted-agent",
    )
    expect(fetchMock.mock.calls[1]?.[1]?.body).toContain("Review results")
    expect(fetchMock.mock.calls[1]?.[1]?.headers).toEqual(
      fetchMock.mock.calls[0]?.[1]?.headers,
    )
    expect(JSON.stringify(payload.input)).toContain("bad-encrypted-agent")
  })

  test("HTTP plaintext recovery stops after one unsuccessful retry", async () => {
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
              encrypted_content: "bad-encrypted-agent",
            },
          ],
        },
      ],
    }
    const failed = {
      type: "response.failed",
      response: {
        error: {
          code: "invalid_request_body",
          message:
            "Encrypted function output content could not be decrypted or decoded.",
        },
      },
    }
    for (const _attempt of [0, 1]) {
      fetchMock.mockResolvedValueOnce(
        new Response(
          `event: response.failed\ndata: ${JSON.stringify(failed)}\n\n`,
        ),
      )
    }
    const response = await createResponses(payload, {
      initiator: "agent",
      requestId: "request-1",
      vision: false,
    })
    if (!isResponsesStream(response)) throw new Error("Expected stream")
    const chunks = await Array.fromAsync(response)

    expect(chunks.map((chunk) => chunk.event)).toEqual(["response.failed"])
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  test("recovers a pre-stream HTTP 400 encrypted agent-message rejection", async () => {
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
              encrypted_content: "bad-encrypted-agent",
            },
          ],
        },
      ],
    }
    fetchMock.mockResolvedValueOnce(
      Response.json(
        {
          error: {
            code: "invalid_request_body",
            message:
              "Encrypted function output content could not be decrypted or decoded.",
          },
        },
        { status: 400 },
      ),
    )
    fetchMock.mockResolvedValueOnce(
      new Response(
        `event: response.completed\ndata: ${JSON.stringify({ type: "response.completed", response: createResponsesResult("gpt-test") })}\n\n`,
        { headers: { "content-type": "text/event-stream" } },
      ),
    )

    const response = await createResponses(payload, {
      initiator: "agent",
      requestId: "request-1",
      sessionId: "interaction-1",
      vision: false,
    })
    if (!isResponsesStream(response)) throw new Error("Expected stream")
    expect(
      (await Array.fromAsync(response)).map((chunk) => chunk.event),
    ).toEqual(["response.completed"])
    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock.mock.calls[1]?.[1]?.body).not.toContain(
      "bad-encrypted-agent",
    )
    expect(fetchMock.mock.calls[1]?.[1]?.body).toContain("Review results")
    expect(fetchMock.mock.calls[1]?.[1]?.headers).toEqual(
      fetchMock.mock.calls[0]?.[1]?.headers,
    )
    expect(JSON.stringify(payload.input)).toContain("bad-encrypted-agent")
  })

  test("preserves HTTP errors and limits pre-stream recovery to one retry", async () => {
    const encryptedPayload: ResponsesPayload = {
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
              encrypted_content: "bad-encrypted-agent",
            },
          ],
        },
      ],
    }
    const error = {
      error: {
        code: "invalid_request_body",
        message:
          "Encrypted function output content could not be decrypted or decoded.",
      },
    }
    const cases = [
      {
        payload: encryptedPayload,
        status: 400,
        body: JSON.stringify(error),
        attempts: 2,
      },
      {
        payload: { ...encryptedPayload, stream: false },
        status: 400,
        body: JSON.stringify(error),
        attempts: 1,
      },
      {
        payload: { ...encryptedPayload, input: "hello" },
        status: 400,
        body: JSON.stringify(error),
        attempts: 1,
      },
      {
        payload: encryptedPayload,
        status: 429,
        body: JSON.stringify(error),
        attempts: 1,
      },
      {
        payload: encryptedPayload,
        status: 400,
        body: JSON.stringify({
          error: { ...error.error, message: "Other invalid input" },
        }),
        attempts: 1,
      },
      { payload: encryptedPayload, status: 400, body: "not-json", attempts: 1 },
    ]
    for (const scenario of cases) {
      fetchMock.mockClear()
      for (let attempt = 0; attempt < scenario.attempts; attempt += 1) {
        fetchMock.mockResolvedValueOnce(
          new Response(scenario.body, { status: scenario.status }),
        )
      }
      let caught: unknown
      try {
        await createResponses(scenario.payload, {
          initiator: "agent",
          requestId: "request-1",
          vision: false,
        })
      } catch (error) {
        caught = error
      }
      expect(caught).toBeInstanceOf(HTTPError)
      if (caught instanceof HTTPError) {
        expect(caught.response.status).toBe(scenario.status)
        expect(await caught.response.text()).toBe(scenario.body)
      }
      expect(fetchMock).toHaveBeenCalledTimes(scenario.attempts)
    }
  })

  test("shares one retry budget between HTTP and streamed failures", async () => {
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
              encrypted_content: "bad-encrypted-agent",
            },
          ],
        },
      ],
    }
    const error = {
      code: "invalid_request_body",
      message:
        "Encrypted function output content could not be decrypted or decoded.",
    }
    fetchMock.mockResolvedValueOnce(Response.json({ error }, { status: 400 }))
    fetchMock.mockResolvedValueOnce(
      new Response(
        `event: response.failed\ndata: ${JSON.stringify({ type: "response.failed", response: { error } })}\n\n`,
      ),
    )
    const response = await createResponses(payload, {
      initiator: "agent",
      requestId: "request-1",
      vision: false,
    })
    if (!isResponsesStream(response)) throw new Error("Expected stream")

    expect(
      (await Array.fromAsync(response)).map((chunk) => chunk.event),
    ).toEqual(["response.failed"])
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  test("builds the first websocket frame as response.create", () => {
    const payload = {
      background: true,
      input: "hello",
      model: "gpt-test",
      service_tier: "auto",
      stream: true,
    } as ResponsesPayload

    const websocketPayload = buildResponsesWebSocketPayload(payload, "agent")

    expect(websocketPayload).toEqual({
      initiator: "agent",
      input: "hello",
      model: "gpt-test",
      type: "response.create",
    })
    expect("stream" in websocketPayload).toBe(false)
    expect("background" in websocketPayload).toBe(false)
    expect("service_tier" in websocketPayload).toBe(false)
  })

  test("builds websocket URLs from the Copilot base URL", () => {
    expect(buildResponsesWebSocketUrl("https://api.githubcopilot.com")).toBe(
      "wss://api.githubcopilot.com/responses",
    )
    expect(buildResponsesWebSocketUrl("http://localhost:3000/")).toBe(
      "ws://localhost:3000/responses",
    )
  })

  test("builds capture-style websocket headers without x-initiator", () => {
    const preparedHeaders = {
      ...copilotHeaders(state, "request-1", true),
      "x-initiator": "user",
    }
    prepareInteractionHeaders("interaction-1", false, preparedHeaders)

    const headers = copilotWebSocketHeaders(preparedHeaders)

    expect(headers).toMatchObject({
      Authorization: "Bearer test-token",
      "Copilot-Integration-Id": "vscode-chat",
      "Copilot-Vision-Request": "true",
      "Editor-Device-Id": "device-1",
      "Editor-Plugin-Version": "copilot-chat/0.67.0",
      "Editor-Version": "vscode/1.120.0",
      "OpenAI-Intent": "conversation-agent",
      "VScode-SessionId": "session-1",
      "VScode-MachineId": "machine-1",
      "X-Agent-Task-Id": "request-1",
      "X-GitHub-Api-Version": "2026-08-01",
      "X-Interaction-Id": "interaction-1",
      "X-Interaction-Type": "conversation-agent",
      "X-Request-Id": "request-1",
      "user-agent": "node",
    })
    const headerNames = Object.keys(headers)
    const agentTaskIdIndex = headerNames.indexOf("X-Agent-Task-Id")
    expect(
      headerNames.slice(agentTaskIdIndex + 1, agentTaskIdIndex + 3),
    ).toEqual(["VScode-SessionId", "VScode-MachineId"])
    expect(headerNames[headerNames.length - 1]).toBe("user-agent")
    expect(headers.accept).toBeUndefined()
    expect(headers["accept-encoding"]).toBeUndefined()
    expect(headers["accept-language"]).toBeUndefined()
    expect(headers["cache-control"]).toBeUndefined()
    expect(headers.pragma).toBeUndefined()
    expect(headers["sec-fetch-mode"]).toBeUndefined()
    expect(headers["x-initiator"]).toBeUndefined()
    expect(headers["sec-websocket-key"]).toBeUndefined()
  })

  test("websocket request uses prepared compact and interaction headers", () => {
    const preparedHeaders = {
      ...copilotHeaders(state, "request-1", false),
      "x-initiator": "user",
    }
    prepareInteractionHeaders("interaction-1", true, preparedHeaders)
    prepareForCompact(preparedHeaders, COMPACT_REQUEST)

    const request = prepareResponsesWebSocketRequest(
      {
        input: "hello",
        model: "gpt-test",
        stream: true,
      },
      preparedHeaders,
      {
        requestId: "request-1",
        subagentMarker: {
          agent_id: "agent-1",
          agent_type: "Explore",
          session_id: "sub-session",
        },
      },
    )

    expect(request.payload).toMatchObject({
      initiator: "agent",
      input: "hello",
      model: "gpt-test",
      type: "response.create",
    })
    expect(request.headers["OpenAI-Intent"]).toBe("conversation-agent")
    expect(request.headers["X-Interaction-Id"]).toBe("interaction-1")
    expect(request.headers["X-Interaction-Type"]).toBe(
      "conversation-compaction",
    )
    expect(request.headers["x-initiator"]).toBeUndefined()
  })

  test("websocket request keeps opencode headers and moves x-initiator into body", () => {
    process.env.COPILOT_API_OAUTH_APP = "opencode"

    const preparedHeaders = {
      ...copilotHeaders(state, "request-1", false),
      "x-initiator": "user",
    }
    prepareInteractionHeaders("interaction-1", true, preparedHeaders)
    prepareForCompact(preparedHeaders, COMPACT_REQUEST)

    const request = prepareResponsesWebSocketRequest(
      {
        input: "hello",
        model: "gpt-test",
        stream: true,
      },
      preparedHeaders,
      {
        requestId: "request-1",
      },
    )

    expect(request.payload.initiator).toBe("agent")
    expect(request.headers).toMatchObject({
      Authorization: "Bearer test-token",
      "Openai-Intent": "conversation-edits",
    })
    expect(request.headers["User-Agent"]).toStartWith("opencode/")
    expect(request.headers["x-initiator"]).toBeUndefined()
    expect(request.headers["X-Request-Id"]).toBeUndefined()
    expect(request.headers["x-interaction-id"]).toBeUndefined()
  })

  test("websocket pool key separates model request and subagent context", () => {
    const basePayload: ResponsesPayload = {
      input: "hello",
      model: "gpt-test",
    }
    const mainKey = buildResponsesWebSocketPoolKey(basePayload, {
      requestId: "request-1",
    })
    const subagentKey = buildResponsesWebSocketPoolKey(basePayload, {
      requestId: "request-1",
      subagentMarker: {
        agent_id: "agent-1",
        agent_type: "Explore",
        session_id: "sub-session",
      },
    })
    const otherModelKey = buildResponsesWebSocketPoolKey(
      {
        ...basePayload,
        model: "gpt-other",
      },
      {
        requestId: "request-1",
      },
    )

    expect(new Set([mainKey, subagentKey, otherModelKey]).size).toBe(3)
    expect(mainKey).toContain("gpt-test")
    expect(mainKey).toContain("request-1")
  })
})

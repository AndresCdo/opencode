import { afterEach, beforeEach, describe, expect, test } from "bun:test"
import { tmpdir } from "../fixture/fixture"
import { resetDatabase } from "../fixture/db"
import { Instance } from "../../src/project/instance"
import { Server } from "../../src/server/server"
import { Flag } from "../../src/flag/flag"
import { GlobalBus } from "../../src/bus/global"

const queue = Flag.OPENCODE_EXPERIMENTAL_EVENT_QUEUE_MAX

beforeEach(async () => {
  await resetDatabase()
})

afterEach(() => {
  // @ts-expect-error test override
  Flag.OPENCODE_EXPERIMENTAL_EVENT_QUEUE_MAX = queue
})

describe("server /global/event", () => {
  test("returns stream.expired for replay request", async () => {
    await using tmp = await tmpdir({ git: true })
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const app = Server.Default()
        const res = await app.request("/global/event?after_seq=0")
        expect(res.status).toBe(200)
        const body = await res.text()
        expect(body).toContain("server.connected")
        expect(body).toContain("server.stream.expired")
        expect(body.indexOf("server.connected")).toBeLessThan(body.indexOf("server.stream.expired"))
      },
    })
  })

  test("returns stream.expired for malformed replay cursor", async () => {
    await using tmp = await tmpdir({ git: true })
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const app = Server.Default()
        const res = await app.request("/global/event?after_seq=bad")
        expect(res.status).toBe(200)
        const body = await res.text()
        expect(body).toContain("server.connected")
        expect(body).toContain("server.stream.expired")
        expect(body.indexOf("server.connected")).toBeLessThan(body.indexOf("server.stream.expired"))
      },
    })
  })

  test("emits stream.lagged on overflow", async () => {
    await using tmp = await tmpdir({ git: true })
    const original = GlobalBus.on
    const originalOff = GlobalBus.off
    try {
      await Instance.provide({
        directory: tmp.path,
        fn: async () => {
          // @ts-expect-error test override
          Flag.OPENCODE_EXPERIMENTAL_EVENT_QUEUE_MAX = 1
          const app = Server.Default()

          GlobalBus.on = ((event: string, handler: (value: unknown) => unknown) => {
            if (event === "event") {
              for (let i = 0; i < 200; i++) {
                handler({
                  directory: "global",
                  payload: {
                    type: "test.global.overflow",
                    properties: { index: i, value: "x".repeat(128) },
                  },
                })
              }
            }
            return GlobalBus
          }) as typeof GlobalBus.on

          GlobalBus.off = (() => GlobalBus) as typeof GlobalBus.off

          const res = await app.request("/global/event")
          expect(res.status).toBe(200)
          const body = await res.text()
          expect(body).toContain("server.stream.lagged")
        },
      })
    } finally {
      GlobalBus.on = original
      GlobalBus.off = originalOff
    }
  })
})

describe("server /global/sync-event", () => {
  test("returns stream.expired for replay request", async () => {
    await using tmp = await tmpdir({ git: true })
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const app = Server.Default()
        const res = await app.request("/global/sync-event?after_seq=0")
        expect(res.status).toBe(200)
        const body = await res.text()
        expect(body).toContain("server.connected")
        expect(body).toContain("server.stream.expired")
        expect(body.indexOf("server.connected")).toBeLessThan(body.indexOf("server.stream.expired"))
      },
    })
  })

  test("returns stream.expired for malformed replay cursor", async () => {
    await using tmp = await tmpdir({ git: true })
    await Instance.provide({
      directory: tmp.path,
      fn: async () => {
        const app = Server.Default()
        const res = await app.request("/global/sync-event?after_seq=bad")
        expect(res.status).toBe(200)
        const body = await res.text()
        expect(body).toContain("server.connected")
        expect(body).toContain("server.stream.expired")
        expect(body.indexOf("server.connected")).toBeLessThan(body.indexOf("server.stream.expired"))
      },
    })
  })
})

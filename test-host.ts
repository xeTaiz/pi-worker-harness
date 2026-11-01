import { mock } from "bun:test";

// Extension registration tests run without the Pi host packages installed.
mock.module("typebox", () => ({
  Type: {
    Object: (properties: Record<string, unknown>, options = {}) => ({ type: "object", properties, ...options }),
    Optional: (schema: Record<string, unknown>) => ({ ...schema, optional: true }),
    String: (options: Record<string, unknown> = {}) => ({ type: "string", ...options }),
    Number: (options: Record<string, unknown> = {}) => ({ type: "number", ...options }),
    Integer: (options: Record<string, unknown> = {}) => ({ type: "integer", ...options }),
    Boolean: (options: Record<string, unknown> = {}) => ({ type: "boolean", ...options }),
    Null: () => ({ type: "null" }),
    Literal: (value: string) => ({ const: value }),
    Union: (anyOf: unknown[]) => ({ anyOf }),
  },
}));

mock.module("@earendil-works/pi-tui", () => ({
  Text: class Text {
    constructor(public text = "") {}
    setText(text: string) {
      this.text = text;
    }
  },
  Container: class Container {
    children: unknown[] = [];
    addChild(child: unknown) {
      this.children.push(child);
    }
    clear() {
      this.children.length = 0;
    }
  },
  Key: { escape: "\u001b", up: "\u001b[A", down: "\u001b[B", right: "\u001b[C", left: "\u001b[D" },
  matchesKey: (data: string, key: string) => data === key,
  truncateToWidth: (text: string, width: number) => text.slice(0, Math.max(0, width)),
}));

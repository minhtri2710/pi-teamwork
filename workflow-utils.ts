import type { EntryRecord } from "@earendil-works/pi-durable";

export function assistantText(entry: EntryRecord | undefined): string {
  const message = entry?.model?.[0];
  return message?.role === "assistant" ? message.content.filter((part) => part.type === "text").map((part) => part.text).join("\n") || "(no assistant output)" : "(no assistant output)";
}

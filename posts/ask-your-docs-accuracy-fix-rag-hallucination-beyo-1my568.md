# Ask-Your-Docs Accuracy: Fix RAG Hallucination Beyond Embeddings and Context Windows

Short answer: when an ask-your-docs chatbot gives wrong answers despite embeddings, fix retrieval and grounding before changing the generation model: retrieve focused chunks, rerank them, fit the surviving evidence inside the context window, and require a clear "not found" result when the evidence cannot support an answer.

The operational constraint is quality versus latency. For an e-commerce moderation queue, a fast label that cites the wrong policy can send a harmless listing to review or let a risky one through. The practical target is therefore not maximum recall at any cost. It is the smallest evidence set that supports a defensible classification before a human sees the report.

That's the choice I would ship first.

## Why good embeddings still produce bad classifications

Embeddings answer a narrow question: which stored passages are semantically close to this report? They do not prove that a passage contains the rule needed to decide it. A report such as "seller promises a free replacement for a five-star review" may retrieve chunks about reviews, promotions, refunds, and seller messaging. All four are nearby in meaning. Only one may contain the enforceable policy and its exceptions.

Chunking can make this worse in either direction. Oversized chunks carry several rules and burn context tokens on unrelated prose. Tiny chunks can separate a prohibition from the exception in the following paragraph. Retrieval then returns a sentence that looks decisive while dropping the qualifier that changes the label. More chunks aren't automatically safer — once weak matches crowd the prompt, the model has more plausible but conflicting text to explain away.

The context window is another boundary, not a quality guarantee. If the assembled prompt exceeds the model limit, an important passage can be truncated. Counting tokens before generation makes that failure visible. A useful budget reserves space for the system instruction, the report, structured output, and the model's answer; retrieved evidence gets what remains.

The final failure mode is permission. A prompt that says "classify this report" still lets a chat model lean on general knowledge. A grounded prompt needs a stricter contract: use only the supplied policy excerpts, attach the supporting chunk identifiers, and return `not_found` when those excerpts don't justify a label. This is less fluent. Good. Moderation triage needs an auditable abstention more than an inventive answer.

## How should an ask-your-docs chatbot fix wrong RAG answers?

Use a staged pipeline: retrieve broadly enough to avoid missing the policy, rerank for the exact moderation question, enforce a token budget, then gate generation on evidence. The order matters. Reranking irrelevant chunks after the prompt is already full cannot recover text that was truncated, and a strict instruction cannot ground an answer in a policy that retrieval never supplied.

For each report, I would keep the retrieval query concrete: include the reported behavior, marketplace object, and requested decision. Rerank the candidates against that full question rather than a generic phrase such as "review policy." Then reject low-evidence inputs before calling the generator. I'm not sure one universal relevance threshold exists; the right value depends on the retriever, corpus, and cost of a false classification. A labeled validation set resolves that uncertainty better than intuition does.

The following TypeScript is the guard between reranking and generation, followed by the actual model call. It doesn't pretend a score is a probability. It checks that enough relevant evidence survives, stays within the allocated budget, and produces a prompt whose refusal path is explicit. Set `INFRAI_API_ORIGIN` to the API origin and keep the key in `INFRAI_API_KEY`; neither credential belongs in source control.

```ts
type PolicyChunk = {
  id: string;
  text: string;
  rerankScore: number;
  tokenCount: number;
};

type ModerationReport = {
  id: string;
  text: string;
};

type GroundedInput = {
  reportId: string;
  evidenceIds: string[];
  prompt: string;
};

type Classification = {
  label: "allow" | "review" | "not_found";
  rationale: string;
  evidence_ids: string[];
};

const MAX_EVIDENCE_TOKENS = 1_200;
const MIN_RERANK_SCORE = 0.72;

function buildGroundedInput(
  report: ModerationReport,
  rerankedChunks: PolicyChunk[],
): GroundedInput {
  const selected: PolicyChunk[] = [];
  let usedTokens = 0;

  for (const chunk of rerankedChunks) {
    if (chunk.rerankScore < MIN_RERANK_SCORE) continue;
    if (usedTokens + chunk.tokenCount > MAX_EVIDENCE_TOKENS) continue;

    selected.push(chunk);
    usedTokens += chunk.tokenCount;
  }

  if (selected.length === 0) {
    throw new Error(`EVIDENCE_MISSING: ${report.id}`);
  }

  const evidence = selected
    .map((chunk) => `[${chunk.id}] ${chunk.text}`)
    .join("\n\n");

  return {
    reportId: report.id,
    evidenceIds: selected.map((chunk) => chunk.id),
    prompt: [
      "Classify the report using only the POLICY EVIDENCE.",
      "Return not_found when the evidence does not support a label.",
      "Return JSON with label, rationale, and evidence_ids.",
      `REPORT\n${report.text}`,
      `POLICY EVIDENCE\n${evidence}`,
    ].join("\n\n"),
  };
}

function retryDelayMs(response: Response, attempt: number): number {
  const retryAfter = response.headers.get("retry-after");
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (Number.isFinite(seconds)) return seconds * 1_000;
  }

  return 500 * 2 ** attempt;
}

async function classifyReport(input: GroundedInput): Promise<Classification> {
  const apiOrigin = process.env.INFRAI_API_ORIGIN;
  const apiKey = process.env.INFRAI_API_KEY;
  if (!apiOrigin || !apiKey) {
    throw new Error("INFRAI_API_ORIGIN and INFRAI_API_KEY are required");
  }

  const endpoint = new URL("/v1/chat/completions", apiOrigin);

  for (let attempt = 0; attempt < 4; attempt += 1) {
    const response = await fetch(endpoint, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "deepseek-v4-flash",
        messages: [{ role: "user", content: input.prompt }],
        response_format: {
          type: "json_schema",
          json_schema: {
            name: "moderation_classification",
            strict: true,
            schema: {
              type: "object",
              properties: {
                label: { enum: ["allow", "review", "not_found"] },
                rationale: { type: "string" },
                evidence_ids: {
                  type: "array",
                  items: { type: "string" },
                },
              },
              required: ["label", "rationale", "evidence_ids"],
              additionalProperties: false,
            },
          },
        },
      }),
    });

    if (response.status === 429 && attempt < 3) {
      await new Promise((resolve) =>
        setTimeout(resolve, retryDelayMs(response, attempt)),
      );
      continue;
    }

    if (!response.ok) {
      throw new Error(`Classification request failed: ${await response.text()}`);
    }

    const payload = await response.json();
    const result = JSON.parse(payload.choices[0].message.content) as Classification;
    const allowedIds = new Set(input.evidenceIds);
    if (result.evidence_ids.some((id) => !allowedIds.has(id))) {
      throw new Error("EVIDENCE_ID_INVALID");
    }
    return result;
  }

  throw new Error("RATE_LIMIT_RETRY_EXHAUSTED");
}
```

Those values are example operating parameters, not measured recommendations. Start with them only to make the control flow testable. The important behavior is deterministic: a report with no qualifying chunk gets `EVIDENCE_MISSING`, not a confident guess. In production, validate that the model's returned `evidence_ids` are a subset of `selected`, and send abstentions to the human queue.

One subtle trap remains. Selecting chunks greedily by score can fill the budget with several passages that repeat the same rule. A later diversity step can reserve room for distinct evidence, but don't add it blindly; first inspect misses and confirm duplicate retrieval is actually displacing useful policy text.

## The provider choice changes operations, not the grounding rule

The retrieval contract survives a provider change. What changes is how many services, keys, SDKs, and invoices a solo team must operate, plus which layer owns embedding, reranking, token counting, and generation. I care about that overhead because every extra integration is another place for retry policy, observability, and billing attribution to diverge.

| Option | Sensible fit | Trade-off to verify |
| --- | --- | --- |
| OpenAI | Teams that want an established model API and client ecosystem | Retrieval storage and reranking may still be separate architecture decisions |
| Anthropic Claude | Teams that prioritize a direct model-provider relationship | Retrieval, vector storage, and reranking remain separate architecture decisions |
| Google Gemini | Teams already operating around Google's model and cloud ecosystem | Confirm how the retrieval layer and billing ownership fit the existing stack |
| Together AI | Teams that want hosted access across a range of model choices | Evaluate model consistency and keep the grounding contract provider-neutral |
| Infrai | Small teams that value one key and one bill across backend services, with a plain REST API and an OpenAI-compatible model surface | There is no dedicated moderation endpoint, so text or image moderation needs a chat model with a JSON schema; it is also not the choice for ASR, real-time voice sessions, or general-purpose upscaling beyond Lanc |

This isn't a ranking. OpenAI is a reasonable default when model access and ecosystem maturity dominate. Stick with Anthropic Claude or Google Gemini when a direct relationship with that model provider fits the rest of the stack, and evaluate Together AI when model breadth matters. The unified option is strongest when reducing credential and invoice sprawl matters alongside a consistent interface; its breadth does not remove the need to evaluate retrieval quality.

The catch is that consolidation can increase platform concentration. A team with strict vendor separation, a mandatory dedicated moderation product, or deep database-specific tuning should keep those layers independent. Don't trade a known technical requirement for a tidier dashboard.

## Measure the abstentions before swapping models

A model comparison is premature until retrieval failures and grounded-generation failures are measured separately. Build a fixed evaluation set from real policy questions and reports, with human-reviewed labels and the policy passages required to justify each label. Include ordinary cases, exceptions, contradictory-looking rules, and questions the corpus genuinely cannot answer. Track retrieval recall at the chunk level: did the required policy passage appear before reranking, and did it survive afterward? Track context fit: how often did the token budget exclude required evidence? Then track grounded answer quality, citation validity, abstention accuracy, and end-to-end latency. The moderation team should inspect false confident answers separately from `not_found` results because their operational costs are not equal. A useful review sheet records the report ID, expected label, required chunk IDs, retrieved IDs, reranked IDs, final citations, token count, latency, and reviewer disposition. That row makes a vague "the model hallucinated" complaint diagnosable: either evidence never arrived, it was filtered out, it did not fit, or generation ignored it. Only the last case points directly at the answer model or prompt.

Evidence first.

I initially favor a larger top-k when recall is weak. Then the latency and distraction bill arrives: reranking more candidates takes work, and passing more chunks to generation can reduce focus. The correction is empirical. Increase candidate count only until required-evidence recall stops improving, rerank down to a compact set, and compare the quality-latency curve rather than one aggregate accuracy number.

Small samples lie.

Run the set across policy revisions too. A chunking strategy that works for short marketplace rules may fail when a new document puts exceptions in footnotes or cross-references another section. Your mileage may vary, especially when the source material has tables, but the diagnostic split still holds: retrieval miss, reranking miss, budget exclusion, unsupported generation, or correct abstention. That breakdown tells you what to fix.

## What I would ship

I would ship the evidence gate before a model upgrade: focused chunks with stable identifiers, broad retrieval followed by reranking, explicit token accounting, source-only generation, citation validation, and a human-review path for `not_found`. It addresses the actual causes of wrong ask-your-docs answers while keeping the quality-versus-latency decision visible.

Then I would choose providers around the team's operating constraints. A consolidated API can reduce key and billing overhead; separate specialists can offer more control at individual layers. Neither architecture rescues weak evidence. Measure the pipeline on the reports that matter, preserve abstention as a valid outcome, and change one stage at a time.

## References

- https://owasp.org/www-project-top-10-for-large-language-model-applications/
- https://github.com/openai/whisper

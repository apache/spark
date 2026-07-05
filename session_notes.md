# Session Notes: Local AI Chat for Spark Docs

## Goal

Build a minimal local RAG chat over Apache Spark documentation in `docs/`.

The first version should stay local and simple:

- index files from `docs/`
- store embeddings in a persistent Chroma database
- answer questions through a CLI
- call a local Ollama model running from Docker Compose
- cite source file paths from the retrieved documentation chunks

## Proposed MVP Shape

Add a small Python tool under the repository root:

```text
ai_docs_chat/
  __init__.py
  config.py
  docs_loader.py
  index_docs.py
  chat.py
docker-compose.ai-docs-chat.yml
```

Generated local state should stay untracked:

```text
.spark-docs-index/
```

Add this directory to `.gitignore`.

## Dependencies

Use lightweight dependencies:

- `chromadb` for persistent vector storage
- `sentence-transformers` for local embeddings
- `beautifulsoup4` for HTML cleanup
- `markdown` or direct text parsing for Markdown if needed
- `requests` for Ollama HTTP calls

Recommended embedding model:

```text
BAAI/bge-small-en-v1.5
```

It is small enough for local development and works well for English technical docs.

Recommended Ollama chat model:

```text
gemma4:e2b
```

If that model name is unavailable locally, the implementation should make the model configurable through an environment variable:

```text
OLLAMA_MODEL=gemma4:e2b
```

## Docker Compose

Add a dedicated compose file instead of changing Spark's existing build/dev flows:

```yaml
services:
  ollama:
    image: ollama/ollama:latest
    ports:
      - "11434:11434"
    volumes:
      - ollama:/root/.ollama

volumes:
  ollama:
```

Usage:

```bash
docker compose -f docker-compose.ai-docs-chat.yml up -d
docker compose -f docker-compose.ai-docs-chat.yml exec ollama ollama pull gemma4:e2b
```

The Python CLI should call:

```text
http://localhost:11434/api/chat
```

## Indexing Flow

`ai_docs_chat/index_docs.py` should:

1. Walk `docs/` recursively.
2. Include only documentation-like files:
   - `.md`
   - `.markdown`
   - `.html`
   - `.rst`
   - `.txt`
3. Skip generated or noisy directories if they appear in docs output.
4. Convert each file to plain text:
   - HTML through BeautifulSoup
   - Markdown/RST/text mostly as raw text for the MVP
5. Split text into overlapping chunks:
   - target chunk size: about 1200-1800 characters for MVP simplicity
   - overlap: about 200 characters
6. Store chunks in Chroma with metadata:
   - `source_path`
   - `chunk_index`
   - optional nearest heading if easy to extract
7. Persist the database under:

```text
.spark-docs-index/chroma
```

Do not try to solve incremental indexing in the MVP. Rebuild the index from scratch.

## Chat Flow

`ai_docs_chat/chat.py` should:

1. Load the existing Chroma collection.
2. Embed the user question with the same embedding model used during indexing.
3. Retrieve top-k chunks, defaulting to `6`.
4. Build a constrained prompt for Ollama.
5. Print:
   - the answer
   - source file paths used by retrieval

The CLI can start as a simple REPL:

```bash
python -m ai_docs_chat.chat
```

Example session:

```text
spark-docs> How do I configure adaptive query execution?
...
Sources:
- docs/sql-performance-tuning.md
- docs/configuration.md
```

## Prompt Contract

Use a strict documentation-grounded prompt:

```text
You answer questions about Apache Spark documentation.

Use only the provided documentation context.
If the context does not contain enough information, say that the provided
documentation excerpts do not contain enough information.
Cite source file paths for important claims.

Context:
{context}

Question:
{question}
```

Each context block should include the path:

```text
[source: docs/path/to/file.md]
chunk text...
```

## Configuration

Use environment variables with conservative defaults:

```text
SPARK_DOCS_CHAT_DOCS_DIR=docs
SPARK_DOCS_CHAT_INDEX_DIR=.spark-docs-index/chroma
SPARK_DOCS_CHAT_EMBED_MODEL=BAAI/bge-small-en-v1.5
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=gemma4:e2b
SPARK_DOCS_CHAT_TOP_K=6
```

Keep configuration in `ai_docs_chat/config.py`.

## Minimal Commands

Index docs:

```bash
python -m ai_docs_chat.index_docs
```

Start Ollama:

```bash
docker compose -f docker-compose.ai-docs-chat.yml up -d
```

Pull model:

```bash
docker compose -f docker-compose.ai-docs-chat.yml exec ollama ollama pull gemma4:e2b
```

Ask questions:

```bash
python -m ai_docs_chat.chat
```

## Expected First Questions

Use these for manual validation:

- How do I configure Spark SQL adaptive query execution?
- What does `spark.sql.shuffle.partitions` do?
- How do I submit a Spark application?
- How does Structured Streaming checkpointing work?
- Where are the Spark Connect docs?

## Acceptance Criteria

The MVP is acceptable when:

- indexing completes against the local `docs/` directory
- a persistent Chroma index is created under `.spark-docs-index/`
- the CLI can answer a question using Ollama
- the answer includes source paths from `docs/`
- the model refuses or qualifies answers when retrieved context is insufficient
- no generated index data is tracked by git

## Non-Goals for the First Version

Do not implement these in the first pass:

- web UI
- authentication
- remote deployment
- pgvector or external databases
- multi-version Spark documentation
- incremental indexing
- agentic tool execution
- fine-tuning
- CI evaluation

## Follow-Up Improvements

After the MVP works:

- add `--rebuild` and `--docs-dir` flags to the indexer
- add `--top-k` and `--model` flags to the chat CLI
- improve Markdown heading extraction
- add retrieval debug mode that prints retrieved chunks and scores
- add a small smoke test for chunking and metadata
- add a README section with setup and usage commands

# Spark Docs AI Chat

This is a minimal local RAG chat for the Apache Spark documentation in `docs/`.

## Setup

Create and activate a Python environment, then install the local chat dependencies:

```bash
pip install -r ai_docs_chat/requirements.txt
```

Start Ollama:

```bash
docker compose -f docker-compose.ai-docs-chat.yml up -d
```

Pull the configured local model:

```bash
docker compose -f docker-compose.ai-docs-chat.yml exec ollama ollama pull gemma4:e2b
```

If that model is not available in your Ollama setup, use another local model:

```bash
export OLLAMA_MODEL=qwen2.5:7b
```

PowerShell:

```powershell
$env:OLLAMA_MODEL = "qwen2.5:7b"
```

## Build the Index

```bash
python -m ai_docs_chat.index_docs
```

The Chroma index is stored in `.spark-docs-index/chroma`, which is ignored by git.

## Chat

Interactive mode:

```bash
python -m ai_docs_chat.chat
```

Ask one question and exit:

```bash
python -m ai_docs_chat.chat "How do I configure adaptive query execution?"
```

Print retrieved chunks for debugging:

```bash
python -m ai_docs_chat.chat --show-context "What does spark.sql.shuffle.partitions do?"
```

## Configuration

Environment variables:

```text
SPARK_DOCS_CHAT_DOCS_DIR=docs
SPARK_DOCS_CHAT_INDEX_DIR=.spark-docs-index/chroma
SPARK_DOCS_CHAT_EMBED_MODEL=BAAI/bge-small-en-v1.5
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=gemma4:e2b
SPARK_DOCS_CHAT_TOP_K=6
```

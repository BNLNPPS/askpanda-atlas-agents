"""LangChain / HuggingFace embedding adapter for DocumentMonitorAgent.

This adapter attempts to create a HuggingFace-based embedder that matches the kind
used by the original AskPanDA VectorStoreManager:
`langchain_community.embeddings.HuggingFaceEmbeddings`.

Behavior:
- Try to import and instantiate a local HuggingFaceEmbeddings (loads a local model).
- If that fails and HUGGINGFACEHUB_API_TOKEN is present, instantiate a hub-backed
  HuggingFaceEmbeddings (remote inference).
- If neither is available, fall back to a safe DummyEmbedder that returns fixed vectors.

The adapter exposes `encode(list[str], show_progress_bar=False) -> List[List[float]]`.
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional

LOG = logging.getLogger(__name__)


class DummyEmbedder:
    """Very small embedder returning fixed dim vectors for dev and CI."""

    def __init__(self, dim: int = 8) -> None:
        self.dim = dim

    def encode(self, texts: List[str], show_progress_bar: bool = False) -> List[List[float]]:
        return [[0.0] * self.dim for _ in texts]


def _instantiate_local_hf(model_name: str):
    """Try to instantiate a local HuggingFace embedder (may require heavy deps)."""
    try:
        # langchain-community wrapper for local models can instantiate from model_name
        from langchain_huggingface import HuggingFaceEmbeddings  # type: ignore
    except Exception as e:
        raise RuntimeError("langchain_community not available") from e

    # Some langchain-community versions use 'model_name' or 'model' parameter name.
    try:
        # Try local instantiation (this will load local transformers/sentence-transformers)
        return HuggingFaceEmbeddings(model_name=model_name)
    except TypeError:
        # fallback different argname
        return HuggingFaceEmbeddings(model=model_name)


def _instantiate_hub_hf(model_name: str, hub_token: Optional[str]):
    """Instantiate HuggingFaceEmbeddings configured to use the HF Hub (remote)."""
    try:
        from langchain_community.embeddings import HuggingFaceEmbeddings  # type: ignore
    except Exception as e:
        raise RuntimeError("langchain_community not available") from e

    kwargs = {}
    if hub_token:
        # different langchain versions use huggingfacehub_api_token vs huggingfacehub_token
        kwargs["huggingfacehub_api_token"] = hub_token
        kwargs["huggingfacehub_token"] = hub_token  # safe to pass extra, wrapper may ignore
    try:
        return HuggingFaceEmbeddings(model_name=model_name, **kwargs)
    except TypeError:
        # fallback names
        return HuggingFaceEmbeddings(model=model_name, **kwargs)


class LangchainHuggingFaceAdapter:
    """Adapter exposing encode(texts) that mirrors AskPanDA's embedding usage.

    Example:
        adapter = LangchainHuggingFaceAdapter(model_name="all-MiniLM-L6-v2")
        embeddings = adapter.encode(["hello world", "another doc"])
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2") -> None:
        self.model_name = model_name
        self._embedder = None
        self._create_embedder()

    def _create_embedder(self) -> None:
        """Try local, then hub, then dummy."""
        # 1) Prefer local instantiation (same semantics as previous AskPanDA code)
        try:
            self._embedder = _instantiate_local_hf(self.model_name)
            LOG.info("Using local HuggingFaceEmbeddings for model '%s'", self.model_name)
            return
        except Exception as e:
            LOG.warning("Local HF instantiation failed (will try hub or dummy): %s", e)

        # 2) If HF hub token available, try remote/hub instantiation
        hub_token = os.getenv("HUGGINGFACEHUB_API_TOKEN") or os.getenv("HF_TOKEN") or os.getenv("HF_API_TOKEN")
        if hub_token:
            try:
                self._embedder = _instantiate_hub_hf(self.model_name, hub_token)
                LOG.info("Using HuggingFaceHub embeddings (remote) for model '%s'", self.model_name)
                return
            except Exception as e:
                LOG.debug("Remote HF instantiation failed: %s", e)

        # 3) fallback to dummy
        LOG.warning("Falling back to DummyEmbedder for embeddings (no HF available).")
        self._embedder = DummyEmbedder(dim=8)

    def encode(self, texts: List[str], show_progress_bar: bool = False) -> List[List[float]]:
        """Return embeddings for each input text via the underlying embedder.

        The underlying langchain embedder often exposes `embed_documents`.
        We support both `embed_documents` and `embed_query` naming variants.
        """
        if not texts:
            return []

        emb = None
        # If the langchain wrapper exposes embed_documents, use it.
        if hasattr(self._embedder, "embed_documents"):
            emb = self._embedder.embed_documents(texts)  # type: ignore
        elif hasattr(self._embedder, "embed_queries"):
            emb = self._embedder.embed_queries(texts)  # type: ignore
        elif hasattr(self._embedder, "embed_query"):
            emb = [self._embedder.embed_query(t) for t in texts]  # type: ignore
        elif hasattr(self._embedder, "encode"):
            # Some wrappers (rare) may provide encode()
            emb = self._embedder.encode(texts, show_progress_bar=show_progress_bar)  # type: ignore
        else:
            raise RuntimeError("Underlying embedder missing embed_documents/embed_query/encode API")

        # Ensure we return Python lists of floats
        result = []
        for e in emb:
            # numpy arrays or other sequences — force to list of floats
            try:
                result.append([float(x) for x in e])
            except Exception:
                # If element is scalar, wrap it
                result.append([float(e)])  # pragma: no cover - defensive

        return result

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import annotations

from dataclasses import dataclass

"""Internal constants."""
_SEQUENCE_MODELS: tuple[str, ...] = (
    "hash2vec",
    "word2vec",
)
_RW_MODELS: tuple[str, ...] = ("rw_with_restart",)
_HASH2VEC_DECAY_FUNCTIONS: tuple[str, ...] = (
    "gaussian",
    "constant",
)


@dataclass
class _RandomWalksEmbeddingsParameters:
    """Internal class represents all possible parameters with defaults."""

    use_edge_direction: bool = False
    rw_model: str = "rw_with_restart"
    rw_max_nbrs: int = 50
    rw_num_walks_per_node: int = 5
    rw_batch_size: int = 10
    rw_num_batches: int = 5
    rw_seed: int = 42
    rw_restart_probability: float = 0.1
    rw_temporary_prefix: str = ""
    rw_cached_walks: str = ""
    sequence_model: str = "hash2vec"
    hash2vec_context_size: int = 5
    hash2vec_num_partitions: int = 5
    hash2vec_embeddings_dim: int = 512
    hash2vec_decay_function: str = "gaussian"
    hash2vec_gaussian_sigma: float = 1.0
    hash2vec_hashing_seed: int = 42
    hash2vec_sign_seed: int = 18
    hash2vec_do_l2_norm: bool = True
    hash2vec_safe_l2: bool = True
    word2vec_max_iter: int = 1
    word2vec_embeddings_dim: int = 100
    word2vec_window_size: int = 5
    word2vec_num_partitions: int = 1
    word2vec_min_count: int = 5
    word2vec_max_sentence_length: int = 1000
    word2vec_seed: int = 42
    word2vec_step_size: float = 0.025
    aggregate_neighbors: bool = True
    aggregate_neighbors_max_nbrs: int = 50
    aggregate_neighbors_seed: int = 42
    clean_up_after_run: bool = False

    def validate(self) -> None:
        if self.sequence_model not in _SEQUENCE_MODELS:
            raise ValueError(f"supported seq2vec models are {str(_SEQUENCE_MODELS)}")
        if self.rw_model not in _RW_MODELS:
            raise ValueError(f"supported RW models are {str(_RW_MODELS)}")

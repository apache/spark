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

"""
Python package for feature in MLlib.
"""
import sys
import warnings
from typing import Dict, Hashable, Iterable, List, Optional, Tuple, Union, overload, TYPE_CHECKING

from py4j.protocol import Py4JJavaError

from pyspark import since
from pyspark.rdd import RDD
from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg import Vectors, _convert_to_vector
from pyspark.mllib.util import JavaLoader, JavaSaveable

from pyspark.context import SparkContext
from pyspark.mllib.linalg import Vector
from pyspark.mllib.regression import LabeledPoint
from py4j.java_collections import JavaMap

if TYPE_CHECKING:
    from pyspark.mllib._typing import VectorLike
    from py4j.java_collections import JavaMap

__all__ = [
    "Normalizer",
    "StandardScalerModel",
    "StandardScaler",
    "HashingTF",
    "IDFModel",
    "IDF",
    "Word2Vec",
    "Word2VecModel",
    "ChiSqSelector",
    "ChiSqSelectorModel",
    "ElementwiseProduct",
]


class VectorTransformer:
    """
    Base class for transformation of a vector or RDD of vector
    """

    @overload
    def transform(self, vector: "VectorLike") -> Vector:
        ...

    @overload
    def transform(self, vector: RDD["VectorLike"]) -> RDD[Vector]:
        ...

    def transform(
        self, vector: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[Vector, RDD[Vector]]:
        """
        Applies transformation on a vector.

        Parameters
        ----------
        vector : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            vector or convertible or RDD to be transformed.
        """
        raise NotImplementedError


class Normalizer(VectorTransformer):
    r"""
    Normalizes samples individually to unit L\ :sup:`p`\  norm

    For any 1 <= `p` < float('inf'), normalizes samples using
    sum(abs(vector) :sup:`p`) :sup:`(1/p)` as norm.

    For `p` = float('inf'), max(abs(vector)) will be used as norm for
    normalization.

    .. versionadded:: 1.2.0

    Parameters
    ----------
    p : float, optional
        Normalization in L^p^ space, p = 2 by default.

    Examples
    --------
    >>> from pyspark.mllib.linalg import Vectors
    >>> v = Vectors.dense(range(3))
    >>> nor = Normalizer(1)
    >>> nor.transform(v)
    DenseVector([0.0, 0.3333, 0.6667])

    >>> rdd = sc.parallelize([v])
    >>> nor.transform(rdd).collect()
    [DenseVector([0.0, 0.3333, 0.6667])]

    >>> nor2 = Normalizer(float("inf"))
    >>> nor2.transform(v)
    DenseVector([0.0, 0.5, 1.0])
    """

    def __init__(self, p: float = 2.0):
        assert p >= 1.0, "p should be greater than 1.0"
        self.p = float(p)

    @overload
    def transform(self, vector: "VectorLike") -> Vector:
        ...

    @overload
    def transform(self, vector: RDD["VectorLike"]) -> RDD[Vector]:
        ...

    def transform(
        self, vector: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[Vector, RDD[Vector]]:
        """
        Applies unit length normalization on a vector.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        vector : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            vector or RDD of vector to be normalized.

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            normalized vector(s). If the norm of the input is zero, it
            will return the input vector.
        """
        if isinstance(vector, RDD):
            vector = vector.map(_convert_to_vector)
        else:
            vector = _convert_to_vector(vector)
        return callMLlibFunc("normalizeVector", self.p, vector)


class JavaVectorTransformer(JavaModelWrapper, VectorTransformer):
    """
    Wrapper for the model in JVM
    """

    @overload
    def transform(self, vector: "VectorLike") -> Vector:
        ...

    @overload
    def transform(self, vector: RDD["VectorLike"]) -> RDD[Vector]:
        ...

    def transform(
        self, vector: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[Vector, RDD[Vector]]:
        """
        Applies transformation on a vector or an RDD[Vector].

        Parameters
        ----------
        vector : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            Input vector(s) to be transformed.

        Notes
        -----
        In Python, transform cannot currently be used within
        an RDD transformation or action.
        Call transform directly on the RDD instead.
        """
        if isinstance(vector, RDD):
            vector = vector.map(_convert_to_vector)
        else:
            vector = _convert_to_vector(vector)
        return self.call("transform", vector)


class StandardScalerModel(JavaVectorTransformer):
    """
    Represents a StandardScaler model that can transform vectors.

    .. versionadded:: 1.2.0
    """

    @overload
    def transform(self, vector: "VectorLike") -> Vector:
        ...

    @overload
    def transform(self, vector: RDD["VectorLike"]) -> RDD[Vector]:
        ...

    def transform(
        self, vector: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[Vector, RDD[Vector]]:
        """
        Applies standardization transformation on a vector.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        vector : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            Input vector(s) to be standardized.

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            Standardized vector(s). If the variance of a column is
            zero, it will return default `0.0` for the column with
            zero variance.

        Notes
        -----
        In Python, transform cannot currently be used within
        an RDD transformation or action.
        Call transform directly on the RDD instead.
        """
        return JavaVectorTransformer.transform(self, vector)

    @since("1.4.0")
    def setWithMean(self, withMean: bool) -> "StandardScalerModel":
        """
        Setter of the boolean which decides
        whether it uses mean or not
        """
        self.call("setWithMean", withMean)
        return self

    @since("1.4.0")
    def setWithStd(self, withStd: bool) -> "StandardScalerModel":
        """
        Setter of the boolean which decides
        whether it uses std or not
        """
        self.call("setWithStd", withStd)
        return self

    @property
    @since("2.0.0")
    def withStd(self) -> bool:
        """
        Returns if the model scales the data to unit standard deviation.
        """
        return self.call("withStd")

    @property
    @since("2.0.0")
    def withMean(self) -> bool:
        """
        Returns if the model centers the data before scaling.
        """
        return self.call("withMean")

    @property
    @since("2.0.0")
    def std(self) -> Vector:
        """
        Return the column standard deviation values.
        """
        return self.call("std")

    @property
    @since("2.0.0")
    def mean(self) -> Vector:
        """
        Return the column mean values.
        """
        return self.call("mean")


class StandardScaler:
    """
    Standardizes features by removing the mean and scaling to unit
    variance using column summary statistics on the samples in the
    training set.

    .. versionadded:: 1.2.0

    Parameters
    ----------
    withMean : bool, optional
        False by default. Centers the data with mean
        before scaling. It will build a dense output, so take
        care when applying to sparse input.
    withStd : bool, optional
        True by default. Scales the data to unit
        standard deviation.

    Examples
    --------
    >>> vs = [Vectors.dense([-2.0, 2.3, 0]), Vectors.dense([3.8, 0.0, 1.9])]
    >>> dataset = sc.parallelize(vs)
    >>> standardizer = StandardScaler(True, True)
    >>> model = standardizer.fit(dataset)
    >>> result = model.transform(dataset)
    >>> for r in result.collect(): r
    DenseVector([-0.7071, 0.7071, -0.7071])
    DenseVector([0.7071, -0.7071, 0.7071])
    >>> int(model.std[0])
    4
    >>> int(model.mean[0]*10)
    9
    >>> model.withStd
    True
    >>> model.withMean
    True
    """

    def __init__(self, withMean: bool = False, withStd: bool = True):
        if not (withMean or withStd):
            warnings.warn("Both withMean and withStd are false. The model does nothing.")
        self.withMean = withMean
        self.withStd = withStd

    def fit(self, dataset: RDD["VectorLike"]) -> "StandardScalerModel":
        """
        Computes the mean and variance and stores as a model to be used
        for later scaling.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.RDD`
            The data used to compute the mean and variance
            to build the transformation model.

        Returns
        -------
        :py:class:`StandardScalerModel`
        """
        dataset = dataset.map(_convert_to_vector)
        jmodel = callMLlibFunc("fitStandardScaler", self.withMean, self.withStd, dataset)
        return StandardScalerModel(jmodel)


class ChiSqSelectorModel(JavaVectorTransformer):
    """
    Represents a Chi Squared selector model.

    .. versionadded:: 1.4.0
    """

    @overload
    def transform(self, vector: "VectorLike") -> Vector:
        ...

    @overload
    def transform(self, vector: RDD["VectorLike"]) -> RDD[Vector]:
        ...

    def transform(
        self, vector: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[Vector, RDD[Vector]]:
        """
        Applies transformation on a vector.

        .. versionadded:: 1.4.0

        Examples
        --------
        vector : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            Input vector(s) to be transformed.

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            transformed vector(s).
        """
        return JavaVectorTransformer.transform(self, vector)


class ChiSqSelector:
    """
    Creates a ChiSquared feature selector.
    The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
    `fdr`, `fwe`.

     * `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.

     * `percentile` is similar but chooses a fraction of all features
       instead of a fixed number.

     * `fpr` chooses all features whose p-values are below a threshold,
       thus controlling the false positive rate of selection.

     * `fdr` uses the `Benjamini-Hochberg procedure <https://en.wikipedia.org/wiki/
       False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure>`_
       to choose all features whose false discovery rate is below a threshold.

     * `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
       1/numFeatures, thus controlling the family-wise error rate of selection.

    By default, the selection method is `numTopFeatures`, with the default number of top features
    set to 50.

    .. versionadded:: 1.4.0

    Examples
    --------
    >>> from pyspark.mllib.linalg import SparseVector, DenseVector
    >>> from pyspark.mllib.regression import LabeledPoint
    >>> data = sc.parallelize([
    ...     LabeledPoint(0.0, SparseVector(3, {0: 8.0, 1: 7.0})),
    ...     LabeledPoint(1.0, SparseVector(3, {1: 9.0, 2: 6.0})),
    ...     LabeledPoint(1.0, [0.0, 9.0, 8.0]),
    ...     LabeledPoint(2.0, [7.0, 9.0, 5.0]),
    ...     LabeledPoint(2.0, [8.0, 7.0, 3.0])
    ... ])
    >>> model = ChiSqSelector(numTopFeatures=1).fit(data)
    >>> model.transform(SparseVector(3, {1: 9.0, 2: 6.0}))
    SparseVector(1, {})
    >>> model.transform(DenseVector([7.0, 9.0, 5.0]))
    DenseVector([7.0])
    >>> model = ChiSqSelector(selectorType="fpr", fpr=0.2).fit(data)
    >>> model.transform(SparseVector(3, {1: 9.0, 2: 6.0}))
    SparseVector(1, {})
    >>> model.transform(DenseVector([7.0, 9.0, 5.0]))
    DenseVector([7.0])
    >>> model = ChiSqSelector(selectorType="percentile", percentile=0.34).fit(data)
    >>> model.transform(DenseVector([7.0, 9.0, 5.0]))
    DenseVector([7.0])
    """

    def __init__(
        self,
        numTopFeatures: int = 50,
        selectorType: str = "numTopFeatures",
        percentile: float = 0.1,
        fpr: float = 0.05,
        fdr: float = 0.05,
        fwe: float = 0.05,
    ):
        self.numTopFeatures = numTopFeatures
        self.selectorType = selectorType
        self.percentile = percentile
        self.fpr = fpr
        self.fdr = fdr
        self.fwe = fwe

    @since("2.1.0")
    def setNumTopFeatures(self, numTopFeatures: int) -> "ChiSqSelector":
        """
        set numTopFeature for feature selection by number of top features.
        Only applicable when selectorType = "numTopFeatures".
        """
        self.numTopFeatures = int(numTopFeatures)
        return self

    @since("2.1.0")
    def setPercentile(self, percentile: float) -> "ChiSqSelector":
        """
        set percentile [0.0, 1.0] for feature selection by percentile.
        Only applicable when selectorType = "percentile".
        """
        self.percentile = float(percentile)
        return self

    @since("2.1.0")
    def setFpr(self, fpr: float) -> "ChiSqSelector":
        """
        set FPR [0.0, 1.0] for feature selection by FPR.
        Only applicable when selectorType = "fpr".
        """
        self.fpr = float(fpr)
        return self

    @since("2.2.0")
    def setFdr(self, fdr: float) -> "ChiSqSelector":
        """
        set FDR [0.0, 1.0] for feature selection by FDR.
        Only applicable when selectorType = "fdr".
        """
        self.fdr = float(fdr)
        return self

    @since("2.2.0")
    def setFwe(self, fwe: float) -> "ChiSqSelector":
        """
        set FWE [0.0, 1.0] for feature selection by FWE.
        Only applicable when selectorType = "fwe".
        """
        self.fwe = float(fwe)
        return self

    @since("2.1.0")
    def setSelectorType(self, selectorType: str) -> "ChiSqSelector":
        """
        set the selector type of the ChisqSelector.
        Supported options: "numTopFeatures" (default), "percentile", "fpr", "fdr", "fwe".
        """
        self.selectorType = str(selectorType)
        return self

    def fit(self, data: RDD[LabeledPoint]) -> "ChiSqSelectorModel":
        """
        Returns a ChiSquared feature selector.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD` of :py:class:`pyspark.mllib.regression.LabeledPoint`
            containing the labeled dataset with categorical features.
            Real-valued features will be treated as categorical for each
            distinct value. Apply feature discretizer before using this function.
        """
        jmodel = callMLlibFunc(
            "fitChiSqSelector",
            self.selectorType,
            self.numTopFeatures,
            self.percentile,
            self.fpr,
            self.fdr,
            self.fwe,
            data,
        )
        return ChiSqSelectorModel(jmodel)


class PCAModel(JavaVectorTransformer):
    """
    Model fitted by [[PCA]] that can project vectors to a low-dimensional space using PCA.

    .. versionadded:: 1.5.0
    """


class PCA:
    """
    A feature transformer that projects vectors to a low-dimensional space using PCA.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> data = [Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),
    ...     Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),
    ...     Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0])]
    >>> model = PCA(2).fit(sc.parallelize(data))
    >>> pcArray = model.transform(Vectors.sparse(5, [(1, 1.0), (3, 7.0)])).toArray()
    >>> pcArray[0]
    1.648...
    >>> pcArray[1]
    -4.013...
    """

    def __init__(self, k: int):
        """
        Parameters
        ----------
        k : int
            number of principal components.
        """
        self.k = int(k)

    def fit(self, data: RDD["VectorLike"]) -> PCAModel:
        """
        Computes a [[PCAModel]] that contains the principal components of the input vectors.

        .. versionadded:: 1.5.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            source vectors
        """
        jmodel = callMLlibFunc("fitPCA", self.k, data)
        return PCAModel(jmodel)


class HashingTF:
    """
    Maps a sequence of terms to their term frequencies using the hashing
    trick.

    .. versionadded:: 1.2.0

    Parameters
    ----------
    numFeatures : int, optional
        number of features (default: 2^20)

    Notes
    -----
    The terms must be hashable (can not be dict/set/list...).

    Examples
    --------
    >>> htf = HashingTF(100)
    >>> doc = "a a b b c d".split(" ")
    >>> htf.transform(doc)
    SparseVector(100, {...})
    """

    def __init__(self, numFeatures: int = 1 << 20):
        self.numFeatures = numFeatures
        self.binary = False

    @since("2.0.0")
    def setBinary(self, value: bool) -> "HashingTF":
        """
        If True, term frequency vector will be binary such that non-zero
        term counts will be set to 1
        (default: False)
        """
        self.binary = value
        return self

    @since("1.2.0")
    def indexOf(self, term: Hashable) -> int:
        """Returns the index of the input term."""
        return hash(term) % self.numFeatures

    @overload
    def transform(self, document: Iterable[Hashable]) -> Vector:
        ...

    @overload
    def transform(self, document: RDD[Iterable[Hashable]]) -> RDD[Vector]:
        ...

    @since("1.2.0")
    def transform(
        self, document: Union[Iterable[Hashable], RDD[Iterable[Hashable]]]
    ) -> Union[Vector, RDD[Vector]]:
        """
        Transforms the input document (list of terms) to term frequency
        vectors, or transform the RDD of document to RDD of term
        frequency vectors.
        """
        if isinstance(document, RDD):
            return document.map(self.transform)

        freq: Dict[int, float] = {}
        for term in document:
            i = self.indexOf(term)
            freq[i] = 1.0 if self.binary else freq.get(i, 0) + 1.0
        return Vectors.sparse(self.numFeatures, freq.items())


class IDFModel(JavaVectorTransformer):
    """
    Represents an IDF model that can transform term frequency vectors.

    .. versionadded:: 1.2.0
    """

    @overload
    def transform(self, x: "VectorLike") -> Vector:
        ...

    @overload
    def transform(self, x: RDD["VectorLike"]) -> RDD[Vector]:
        ...

    def transform(self, x: Union["VectorLike", RDD["VectorLike"]]) -> Union[Vector, RDD[Vector]]:
        """
        Transforms term frequency (TF) vectors to TF-IDF vectors.

        If `minDocFreq` was set for the IDF calculation,
        the terms which occur in fewer than `minDocFreq`
        documents will have an entry of 0.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        x : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            an RDD of term frequency vectors or a term frequency
            vector

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            an RDD of TF-IDF vectors or a TF-IDF vector

        Notes
        -----
        In Python, transform cannot currently be used within
        an RDD transformation or action.
        Call transform directly on the RDD instead.
        """
        return JavaVectorTransformer.transform(self, x)

    @since("1.4.0")
    def idf(self) -> Vector:
        """
        Returns the current IDF vector.
        """
        return self.call("idf")

    @since("3.0.0")
    def docFreq(self) -> List[int]:
        """
        Returns the document frequency.
        """
        return self.call("docFreq")

    @since("3.0.0")
    def numDocs(self) -> int:
        """
        Returns number of documents evaluated to compute idf
        """
        return self.call("numDocs")


class IDF:
    """
    Inverse document frequency (IDF).

    The standard formulation is used: `idf = log((m + 1) / (d(t) + 1))`,
    where `m` is the total number of documents and `d(t)` is the number
    of documents that contain term `t`.

    This implementation supports filtering out terms which do not appear
    in a minimum number of documents (controlled by the variable
    `minDocFreq`). For terms that are not in at least `minDocFreq`
    documents, the IDF is found as 0, resulting in TF-IDFs of 0.

    .. versionadded:: 1.2.0

    Parameters
    ----------
    minDocFreq : int
        minimum of documents in which a term should appear for filtering

    Examples
    --------
    >>> n = 4
    >>> freqs = [Vectors.sparse(n, (1, 3), (1.0, 2.0)),
    ...          Vectors.dense([0.0, 1.0, 2.0, 3.0]),
    ...          Vectors.sparse(n, [1], [1.0])]
    >>> data = sc.parallelize(freqs)
    >>> idf = IDF()
    >>> model = idf.fit(data)
    >>> tfidf = model.transform(data)
    >>> for r in tfidf.collect(): r
    SparseVector(4, {1: 0.0, 3: 0.5754})
    DenseVector([0.0, 0.0, 1.3863, 0.863])
    SparseVector(4, {1: 0.0})
    >>> model.transform(Vectors.dense([0.0, 1.0, 2.0, 3.0]))
    DenseVector([0.0, 0.0, 1.3863, 0.863])
    >>> model.transform([0.0, 1.0, 2.0, 3.0])
    DenseVector([0.0, 0.0, 1.3863, 0.863])
    >>> model.transform(Vectors.sparse(n, (1, 3), (1.0, 2.0)))
    SparseVector(4, {1: 0.0, 3: 0.5754})
    """

    def __init__(self, minDocFreq: int = 0):
        self.minDocFreq = minDocFreq

    def fit(self, dataset: RDD["VectorLike"]) -> IDFModel:
        """
        Computes the inverse document frequency.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.RDD`
            an RDD of term frequency vectors
        """
        if not isinstance(dataset, RDD):
            raise TypeError("dataset should be an RDD of term frequency vectors")
        jmodel = callMLlibFunc("fitIDF", self.minDocFreq, dataset.map(_convert_to_vector))
        return IDFModel(jmodel)


class Word2VecModel(JavaVectorTransformer, JavaSaveable, JavaLoader["Word2VecModel"]):
    """
    class for Word2Vec model
    """

    def transform(self, word: str) -> Vector:  # type: ignore[override]
        """
        Transforms a word to its vector representation

        .. versionadded:: 1.2.0

        Parameters
        ----------
        word : str
            a word

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.Vector`
            vector representation of word(s)

        Notes
        -----
        Local use only
        """
        try:
            return self.call("transform", word)
        except Py4JJavaError:
            raise ValueError("%s not found" % word)

    def findSynonyms(self, word: Union[str, "VectorLike"], num: int) -> Iterable[Tuple[str, float]]:
        """
        Find synonyms of a word

        .. versionadded:: 1.2.0

        Parameters
        ----------

        word : str or  :py:class:`pyspark.mllib.linalg.Vector`
            a word or a vector representation of word
        num : int
            number of synonyms to find

        Returns
        -------
        :py:class:`collections.abc.Iterable`
            array of (word, cosineSimilarity)

        Notes
        -----
        Local use only
        """
        if not isinstance(word, str):
            word = _convert_to_vector(word)
        words, similarity = self.call("findSynonyms", word, num)
        return zip(words, similarity)

    @since("1.4.0")
    def getVectors(self) -> "JavaMap":
        """
        Returns a map of words to their vector representations.
        """
        return self.call("getVectors")

    @classmethod
    @since("1.5.0")
    def load(cls, sc: SparkContext, path: str) -> "Word2VecModel":
        """
        Load a model from the given path.
        """
        assert sc._jvm is not None

        jmodel = sc._jvm.org.apache.spark.mllib.feature.Word2VecModel.load(sc._jsc.sc(), path)
        model = sc._jvm.org.apache.spark.mllib.api.python.Word2VecModelWrapper(jmodel)
        return Word2VecModel(model)


class Word2Vec:
    """Word2Vec creates vector representation of words in a text corpus.
    The algorithm first constructs a vocabulary from the corpus
    and then learns vector representation of words in the vocabulary.
    The vector representation can be used as features in
    natural language processing and machine learning algorithms.

    We used skip-gram model in our implementation and hierarchical
    softmax method to train the model. The variable names in the
    implementation matches the original C implementation.

    For original C implementation,
    see https://code.google.com/p/word2vec/
    For research papers, see
    Efficient Estimation of Word Representations in Vector Space
    and Distributed Representations of Words and Phrases and their
    Compositionality.

    .. versionadded:: 1.2.0

    Examples
    --------
    >>> sentence = "a b " * 100 + "a c " * 10
    >>> localDoc = [sentence, sentence]
    >>> doc = sc.parallelize(localDoc).map(lambda line: line.split(" "))
    >>> model = Word2Vec().setVectorSize(10).setSeed(42).fit(doc)

    Querying for synonyms of a word will not return that word:

    >>> syms = model.findSynonyms("a", 2)
    >>> [s[0] for s in syms]
    ['b', 'c']

    But querying for synonyms of a vector may return the word whose
    representation is that vector:

    >>> vec = model.transform("a")
    >>> syms = model.findSynonyms(vec, 2)
    >>> [s[0] for s in syms]
    ['a', 'b']

    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = Word2VecModel.load(sc, path)
    >>> model.transform("a") == sameModel.transform("a")
    True
    >>> syms = sameModel.findSynonyms("a", 2)
    >>> [s[0] for s in syms]
    ['b', 'c']
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass
    """

    def __init__(self) -> None:
        """
        Construct Word2Vec instance
        """
        self.vectorSize = 100
        self.learningRate = 0.025
        self.numPartitions = 1
        self.numIterations = 1
        self.seed: Optional[int] = None
        self.minCount = 5
        self.windowSize = 5

    @since("1.2.0")
    def setVectorSize(self, vectorSize: int) -> "Word2Vec":
        """
        Sets vector size (default: 100).
        """
        self.vectorSize = vectorSize
        return self

    @since("1.2.0")
    def setLearningRate(self, learningRate: float) -> "Word2Vec":
        """
        Sets initial learning rate (default: 0.025).
        """
        self.learningRate = learningRate
        return self

    @since("1.2.0")
    def setNumPartitions(self, numPartitions: int) -> "Word2Vec":
        """
        Sets number of partitions (default: 1). Use a small number for
        accuracy.
        """
        self.numPartitions = numPartitions
        return self

    @since("1.2.0")
    def setNumIterations(self, numIterations: int) -> "Word2Vec":
        """
        Sets number of iterations (default: 1), which should be smaller
        than or equal to number of partitions.
        """
        self.numIterations = numIterations
        return self

    @since("1.2.0")
    def setSeed(self, seed: int) -> "Word2Vec":
        """
        Sets random seed.
        """
        self.seed = seed
        return self

    @since("1.4.0")
    def setMinCount(self, minCount: int) -> "Word2Vec":
        """
        Sets minCount, the minimum number of times a token must appear
        to be included in the word2vec model's vocabulary (default: 5).
        """
        self.minCount = minCount
        return self

    @since("2.0.0")
    def setWindowSize(self, windowSize: int) -> "Word2Vec":
        """
        Sets window size (default: 5).
        """
        self.windowSize = windowSize
        return self

    def fit(self, data: RDD[List[str]]) -> "Word2VecModel":
        """
        Computes the vector representation of each word in vocabulary.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            training data. RDD of list of string

        Returns
        -------
        :py:class:`Word2VecModel`
        """
        if not isinstance(data, RDD):
            raise TypeError("data should be an RDD of list of string")
        jmodel = callMLlibFunc(
            "trainWord2VecModel",
            data,
            int(self.vectorSize),
            float(self.learningRate),
            int(self.numPartitions),
            int(self.numIterations),
            self.seed,
            int(self.minCount),
            int(self.windowSize),
        )
        return Word2VecModel(jmodel)


class ElementwiseProduct(VectorTransformer):
    """
    Scales each column of the vector, with the supplied weight vector.
    i.e the elementwise product.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> weight = Vectors.dense([1.0, 2.0, 3.0])
    >>> eprod = ElementwiseProduct(weight)
    >>> a = Vectors.dense([2.0, 1.0, 3.0])
    >>> eprod.transform(a)
    DenseVector([2.0, 2.0, 9.0])
    >>> b = Vectors.dense([9.0, 3.0, 4.0])
    >>> rdd = sc.parallelize([a, b])
    >>> eprod.transform(rdd).collect()
    [DenseVector([2.0, 2.0, 9.0]), DenseVector([9.0, 6.0, 12.0])]
    """

    def __init__(self, scalingVector: Vector) -> None:
        self.scalingVector = _convert_to_vector(scalingVector)

    @overload
    def transform(self, vector: "VectorLike") -> Vector:
        ...

    @overload
    def transform(self, vector: RDD["VectorLike"]) -> RDD[Vector]:
        ...

    def transform(
        self, vector: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[Vector, RDD[Vector]]:
        """
        Computes the Hadamard product of the vector.

        .. versionadded:: 1.5.0
        """
        if isinstance(vector, RDD):
            vector = vector.map(_convert_to_vector)

        else:
            vector = _convert_to_vector(vector)
        return callMLlibFunc("elementwiseProductVector", self.scalingVector, vector)


def _test() -> None:
    import doctest
    from pyspark.sql import SparkSession

    globs = globals().copy()
    spark = SparkSession.builder.master("local[4]").appName("mllib.feature tests").getOrCreate()
    globs["sc"] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    sys.path.pop(0)
    _test()

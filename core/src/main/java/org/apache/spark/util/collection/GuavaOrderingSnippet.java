package org.apache.spark.util.collection;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.math.IntMath;

import javax.annotation.CheckForNull;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.spark.util.collection.GuavaOrderingSnippet.NullnessCasts.uncheckedCastNullableTToT;

/**
 * Code snippet of {@code Ordering} from guava-32.1.3 to resolve performance issue on {@code leastOf} method.
 *
 * <p>See <a href="https://olapio.atlassian.net/browse/AL-9236">AL-9236</a> for more details.
 */
public abstract class GuavaOrderingSnippet<T extends Object> implements Comparator<T> {

    /**
     * Returns the {@code k} least elements of the given iterable according to this ordering, in order
     * from least to greatest. If there are fewer than {@code k} elements present, all will be
     * included.
     *
     * <p>The implementation does not necessarily use a <i>stable</i> sorting algorithm; when multiple
     * elements are equivalent, it is undefined which will come first.
     *
     * <p><b>Java 8 users:</b> Use {@code Streams.stream(iterable).collect(Comparators.least(k,
     * thisComparator))} instead.
     *
     * @return an immutable {@code RandomAccess} list of the {@code k} least elements in ascending
     * order
     * @throws IllegalArgumentException if {@code k} is negative
     * @since 8.0
     */
    public <E extends T> List<E> leastOf(Iterable<E> iterable, int k) {
        if (iterable instanceof Collection) {
            Collection<E> collection = (Collection<E>) iterable;
            if (collection.size() <= 2L * k) {
                // In this case, just dumping the collection to an array and sorting is
                // faster than using the implementation for Iterator, which is
                // specialized for k much smaller than n.

                @SuppressWarnings("unchecked") // c only contains E's and doesn't escape
                E[] array = (E[]) collection.toArray();
                Arrays.sort(array, this);
                if (array.length > k) {
                    array = Arrays.copyOf(array, k);
                }
                return Collections.unmodifiableList(Arrays.asList(array));
            }
        }
        return leastOf(iterable.iterator(), k);
    }

    /**
     * Returns the {@code k} least elements from the given iterator according to this ordering, in
     * order from least to greatest. If there are fewer than {@code k} elements present, all will be
     * included.
     *
     * <p>The implementation does not necessarily use a <i>stable</i> sorting algorithm; when multiple
     * elements are equivalent, it is undefined which will come first.
     *
     * <p><b>Java 8 users:</b> Use {@code Streams.stream(iterator).collect(Comparators.least(k,
     * thisComparator))} instead.
     *
     * @return an immutable {@code RandomAccess} list of the {@code k} least elements in ascending
     * order
     * @throws IllegalArgumentException if {@code k} is negative
     * @since 14.0
     */
    public <E extends T> List<E> leastOf(Iterator<E> iterator, int k) {
        checkNotNull(iterator);
        checkNonnegative(k, "k");

        if (k == 0 || !iterator.hasNext()) {
            return Collections.emptyList();
        } else if (k >= Integer.MAX_VALUE / 2) {
            // k is really large; just do a straightforward sorted-copy-and-sublist
            ArrayList<E> list = Lists.newArrayList(iterator);
            Collections.sort(list, this);
            if (list.size() > k) {
                list.subList(k, list.size()).clear();
            }
            list.trimToSize();
            return Collections.unmodifiableList(list);
        } else {
            TopKSelector<E> selector = TopKSelector.least(k, this);
            selector.offerAll(iterator);
            return selector.topK();
        }
    }

    static int checkNonnegative(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " cannot be negative but was: " + value);
        }
        return value;
    }

    static final class TopKSelector<T extends Object> {

        /**
         * Returns a {@code TopKSelector} that collects the lowest {@code k} elements added to it,
         * relative to the specified comparator, and returns them via {@link #topK} in ascending order.
         *
         * @throws IllegalArgumentException if {@code k < 0} or {@code k > Integer.MAX_VALUE / 2}
         */
        public static <T extends Object> TopKSelector<T> least(
                int k, Comparator<? super T> comparator) {
            return new TopKSelector<T>(comparator, k);
        }

        private final int k;
        private final Comparator<? super T> comparator;

        /*
         * We are currently considering the elements in buffer in the range [0, bufferSize) as candidates
         * for the top k elements. Whenever the buffer is filled, we quickselect the top k elements to the
         * range [0, k) and ignore the remaining elements.
         */
        private final T[] buffer;
        private int bufferSize;

        /**
         * The largest of the lowest k elements we've seen so far relative to this comparator. If
         * bufferSize ≥ k, then we can ignore any elements greater than this value.
         */
        @CheckForNull
        private T threshold;

        private TopKSelector(Comparator<? super T> comparator, int k) {
            this.comparator = checkNotNull(comparator, "comparator");
            this.k = k;
            checkArgument(k >= 0, "k (%s) must be >= 0", k);
            checkArgument(k <= Integer.MAX_VALUE / 2, "k (%s) must be <= Integer.MAX_VALUE / 2", k);
            this.buffer = (T[]) new Object[IntMath.checkedMultiply(k, 2)];
            this.bufferSize = 0;
            this.threshold = null;
        }

        /**
         * Adds {@code elem} as a candidate for the top {@code k} elements. This operation takes amortized
         * O(1) time.
         */
        public void offer(T elem) {
            if (k == 0) {
                return;
            } else if (bufferSize == 0) {
                buffer[0] = elem;
                threshold = elem;
                bufferSize = 1;
            } else if (bufferSize < k) {
                buffer[bufferSize++] = elem;
                // uncheckedCastNullableTToT is safe because bufferSize > 0.
                if (comparator.compare(elem, uncheckedCastNullableTToT(threshold)) > 0) {
                    threshold = elem;
                }
                // uncheckedCastNullableTToT is safe because bufferSize > 0.
            } else if (comparator.compare(elem, uncheckedCastNullableTToT(threshold)) < 0) {
                // Otherwise, we can ignore elem; we've seen k better elements.
                buffer[bufferSize++] = elem;
                if (bufferSize == 2 * k) {
                    trim();
                }
            }
        }

        /**
         * Quickselects the top k elements from the 2k elements in the buffer. O(k) expected time, O(k log
         * k) worst case.
         */
        private void trim() {
            int left = 0;
            int right = 2 * k - 1;

            int minThresholdPosition = 0;
            // The leftmost position at which the greatest of the k lower elements
            // -- the new value of threshold -- might be found.

            int iterations = 0;
            int maxIterations = IntMath.log2(right - left, RoundingMode.CEILING) * 3;
            while (left < right) {
                int pivotIndex = (left + right + 1) >>> 1;

                int pivotNewIndex = partition(left, right, pivotIndex);

                if (pivotNewIndex > k) {
                    right = pivotNewIndex - 1;
                } else if (pivotNewIndex < k) {
                    left = Math.max(pivotNewIndex, left + 1);
                    minThresholdPosition = pivotNewIndex;
                } else {
                    break;
                }
                iterations++;
                if (iterations >= maxIterations) {
                    @SuppressWarnings("nullness") // safe because we pass sort() a range that contains real Ts
                    T[] castBuffer = (T[]) buffer;
                    // We've already taken O(k log k), let's make sure we don't take longer than O(k log k).
                    Arrays.sort(castBuffer, left, right + 1, comparator);
                    break;
                }
            }
            bufferSize = k;

            threshold = uncheckedCastNullableTToT(buffer[minThresholdPosition]);
            for (int i = minThresholdPosition + 1; i < k; i++) {
                if (comparator.compare(
                        uncheckedCastNullableTToT(buffer[i]), uncheckedCastNullableTToT(threshold))
                        > 0) {
                    threshold = buffer[i];
                }
            }
        }

        /**
         * Partitions the contents of buffer in the range [left, right] around the pivot element
         * previously stored in buffer[pivotValue]. Returns the new index of the pivot element,
         * pivotNewIndex, so that everything in [left, pivotNewIndex] is ≤ pivotValue and everything in
         * (pivotNewIndex, right] is greater than pivotValue.
         */
        private int partition(int left, int right, int pivotIndex) {
            T pivotValue = uncheckedCastNullableTToT(buffer[pivotIndex]);
            buffer[pivotIndex] = buffer[right];

            int pivotNewIndex = left;
            for (int i = left; i < right; i++) {
                if (comparator.compare(uncheckedCastNullableTToT(buffer[i]), pivotValue) < 0) {
                    swap(pivotNewIndex, i);
                    pivotNewIndex++;
                }
            }
            buffer[right] = buffer[pivotNewIndex];
            buffer[pivotNewIndex] = pivotValue;
            return pivotNewIndex;
        }

        private void swap(int i, int j) {
            T tmp = buffer[i];
            buffer[i] = buffer[j];
            buffer[j] = tmp;
        }

        TopKSelector<T> combine(TopKSelector<T> other) {
            for (int i = 0; i < other.bufferSize; i++) {
                this.offer(uncheckedCastNullableTToT(other.buffer[i]));
            }
            return this;
        }

        /**
         * Adds each member of {@code elements} as a candidate for the top {@code k} elements. This
         * operation takes amortized linear time in the length of {@code elements}.
         *
         * <p>If all input data to this {@code TopKSelector} is in a single {@code Iterable}, prefer
         * {@link Ordering#leastOf(Iterable, int)}, which provides a simpler API for that use case.
         */
        public void offerAll(Iterable<? extends T> elements) {
            offerAll(elements.iterator());
        }

        /**
         * Adds each member of {@code elements} as a candidate for the top {@code k} elements. This
         * operation takes amortized linear time in the length of {@code elements}. The iterator is
         * consumed after this operation completes.
         *
         * <p>If all input data to this {@code TopKSelector} is in a single {@code Iterator}, prefer
         * {@link Ordering#leastOf(Iterator, int)}, which provides a simpler API for that use case.
         */
        public void offerAll(Iterator<? extends T> elements) {
            while (elements.hasNext()) {
                offer(elements.next());
            }
        }

        /**
         * Returns the top {@code k} elements offered to this {@code TopKSelector}, or all elements if
         * fewer than {@code k} have been offered, in the order specified by the factory used to create
         * this {@code TopKSelector}.
         *
         * <p>The returned list is an unmodifiable copy and will not be affected by further changes to
         * this {@code TopKSelector}. This method returns in O(k log k) time.
         */
        public List<T> topK() {
            @SuppressWarnings("nullness") // safe because we pass sort() a range that contains real Ts
            T[] castBuffer = (T[]) buffer;
            Arrays.sort(castBuffer, 0, bufferSize, comparator);
            if (bufferSize > k) {
                Arrays.fill(buffer, k, buffer.length, null);
                bufferSize = k;
                threshold = buffer[k - 1];
            }
            // Up to bufferSize, all elements of buffer are real Ts (not null unless T includes null)
            T[] topK = Arrays.copyOf(castBuffer, bufferSize);
            // we have to support null elements, so no ImmutableList for us
            return Collections.unmodifiableList(Arrays.asList(topK));
        }
    }

    static final class NullnessCasts {

        @SuppressWarnings("nullness")
        static <T extends Object> T uncheckedCastNullableTToT(@CheckForNull T t) {
            return t;
        }

        /**
         * Returns {@code null} as any type, even one that does not include {@code null}.
         */
        @SuppressWarnings({"nullness", "TypeParameterUnusedInFormals", "ReturnMissingNullable"})
        // The warnings are legitimate. Each time we use this method, we document why.
        static <T extends Object> T unsafeNull() {
            return null;
        }

        private NullnessCasts() {
        }
    }
}

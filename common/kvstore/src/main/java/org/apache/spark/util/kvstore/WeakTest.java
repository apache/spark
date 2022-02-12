package org.apache.spark.util.kvstore;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

public class WeakTest {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentLinkedQueue<Reference<TestObject>> iteratorTracker = new ConcurrentLinkedQueue<>();
        iteratorTracker.add(new WeakReference<>(new TestObject(1)));
        iteratorTracker.add(new WeakReference<>(new TestObject(2)));
        System.gc();
        iteratorTracker.add(new WeakReference<>(new TestObject(3)));
        iteratorTracker.add(new WeakReference<>(new TestObject(4)));
        iteratorTracker.add(new WeakReference<>(new TestObject(5)));
        iteratorTracker.removeIf(ref -> ref.get() == null || 3 == Objects.requireNonNull(ref.get()).value);
        for (Reference<TestObject> ref: iteratorTracker) {
            System.out.println(ref.get());
        }
        Thread.sleep(1000);
    }

    static class TestObject {
        private Integer value;
        public TestObject(Integer value) {
            this.value = value;
        }

        @Override
        protected void finalize() throws Throwable {
            Thread.sleep(50);
            System.out.println("value is " + value);
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }
}

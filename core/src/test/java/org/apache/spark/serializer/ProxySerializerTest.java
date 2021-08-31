package org.apache.spark.serializer;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

class ContainsProxyClass implements Serializable {
    final MyInterface proxy = (MyInterface) Proxy.newProxyInstance(
            MyInterface.class.getClassLoader(),
            new Class[]{MyInterface.class},
            new MyInvocationHandler());

    public interface MyInterface {
        void myMethod();
    }

    static class MyClass implements MyInterface, Serializable {
        @Override
        public void myMethod() {
        }
    }

    class MyInvocationHandler implements InvocationHandler, Serializable {
        private final MyClass real = new MyClass();

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return method.invoke(real, args);
        }
    }
}

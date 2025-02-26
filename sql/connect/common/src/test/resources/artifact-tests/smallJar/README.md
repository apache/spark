A simple Hello World program used for testing.

## Source Code

Located in `src/org/apache/test/jar/HelloWorld.java`

## Build Instructions

1. Compile the source:

```bash
javac src/org/apache/test/jar/HelloWorld.java
```

2. Create JAR with the manifest:

```bash
jar cvfm smallJar.jar manifest.txt -C src .
```

3. Test the JAR:

```bash
java -jar smallJar.jar
```
// Compile: javac --source 8 --target 8 HelloWithPackage.java

package my.custom.pkg;

public class HelloWithPackage {
    String name = "there";

    public HelloWithPackage() {
    }

    public HelloWithPackage(String name) {
        this.name = name;
    }

    public String msg() {
        return "Hello " + name + "! Nice to meet you!";
    }
}

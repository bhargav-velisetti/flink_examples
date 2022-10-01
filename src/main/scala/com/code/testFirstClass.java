package com.code;

public class testFirstClass {

    public int a;
    public String b;

    public testFirstClass(int a, String b) {
        this.a = a;
        this.b = b;
    }

    public testFirstClass() {
        this.a = 0;
        this.b = "NA";
    }

    public static void main(String[] args) {
        testFirstClass xyz = new testFirstClass();
        System.out.println("first var is " + xyz.a + " second var is " + xyz.b);
        xyz.setA(1);
        xyz.setB("Hello");
        System.out.println("first var is " + xyz.a + " second var is " + xyz.b);
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public String getB() {
        return b;
    }

    public void setB(String b) {
        this.b = b;
    }

}

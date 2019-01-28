package solutions.boono.redisdispatcher.tests;

public class TestMessage {
    private String field1;
    private Integer field2;

    public String getField1() {
        return field1;
    }

    void setField1(String field1) {
        this.field1 = field1;
    }

    public Integer getField2() {
        return field2;
    }

    void setField2(Integer field2) {
        this.field2 = field2;
    }

    @Override
    public String toString() {
        return "f1: " + field1 + ", f2: " + field2;
    }
}

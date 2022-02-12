package simpledb.query;

public class OrderInfo {
    private String field;
    private String orderType;

    public OrderInfo(String field, String orderType) {
        this.field = field;
        this.orderType = orderType;
    }

    public String getField() {
        return field;
    }

    public boolean isAsc() {
        return orderType.equals("asc");
    }
}

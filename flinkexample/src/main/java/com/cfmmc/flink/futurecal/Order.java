package com.cfmmc.flink.futurecal;

import com.alibaba.fastjson.JSONObject;

import java.util.*;

public class Order {
    private String name;
    private double price;
    private String futureVariety;  // 期货类型
    private String userId;
    private long timestamp;
    private int number;    // 手数
    private double unit;   // 单位
    private long timestampSecond; //生成数据的时间
    private double orderPay;  // orderpay = price*nmber


    public long getTimestampSecond() {
        return timestampSecond;
    }

    public void setTimestampSecond(long timestampSecond) {
        this.timestampSecond = timestampSecond;
    }

    public double getOrderPay() {
        return orderPay;
    }

    public void setOrderPay(double orderPay) {
        this.orderPay = orderPay;
    }




    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getFutureVariety() {
        return futureVariety;
    }

    public void setFutureVariety(String futureVariety) {
        this.futureVariety = futureVariety;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public double getUnit() {
        return unit;
    }

    public void setUnit(double unit) {
        this.unit = unit;
    }

    public static List<Order> BuildMockData(int count) {
        List<Order> orderList = new ArrayList<Order>();

        //品种集合
        List<String> futureVarietyList = new ArrayList<String>();
        futureVarietyList.add("cu2005");
        futureVarietyList.add("jd2005");
        futureVarietyList.add("sr2005");

        // 价格区间
        Map<String, FutureConfig> futureMap = new HashMap<String, FutureConfig>();
        futureMap.put("cu2005", new FutureConfig(600, 650, (double)1));
        futureMap.put("jd2005", new FutureConfig(3200, 3300, (double)0.5));
        futureMap.put("sr2005", new FutureConfig(2600, 2700, (double)0.5));

        for (int i = 0; i < count; i++) {
            orderList.add(BuildRandomOrder(futureVarietyList, futureMap));
        }
        return orderList;
    }

    private static Order BuildRandomOrder(List<String> futureVarietyList, Map<String, FutureConfig> futureMap) {
        Order order = new Order();
        Random random = new Random();
        Date dateTime = new Date();

        int varietyIndex = random.nextInt(futureVarietyList.size());
        order.futureVariety = futureVarietyList.get(varietyIndex);
        order.name = String.valueOf(dateTime.getTime()) + String.valueOf(random.nextInt(100000));
        order.number = random.nextInt(100) + 1;

        FutureConfig futureConfig = futureMap.get(order.futureVariety);
        order.price = random.nextInt(futureConfig.getPriceUpperBound() - futureConfig.getPriceLowerBound()) + futureConfig.getPriceLowerBound();

        order.timestamp = new Date().getTime();
        order.unit = futureConfig.getUnit();
        order.userId = "User" + random.nextInt(100);

        return order;
    }

    public static void main(String[] args) {
        List<Order> orderList = BuildMockData(100);
        for (Order order: orderList) {
            System.out.println(JSONObject.toJSONString(order));

        }
    }

    @Override
    public String toString() {
        return "Order{" +
                "name='" + name + '\'' +
                ", price=" + price +
                ", futureVariety='" + futureVariety + '\'' +
                ", userId='" + userId + '\'' +
                ", timestamp=" + timestamp +
                ", number=" + number +
                ", unit=" + unit +
                ", timestampSecond=" + timestampSecond +
                ", orderPay=" + orderPay +
                '}';
    }
}

class FutureConfig {
    private int priceLowerBound;
    private int priceUpperBound;
    private double unit;

    public FutureConfig(int priceLowerBound, int priceUpperBound, double unit) {
        this.priceLowerBound = priceLowerBound;
        this.priceUpperBound = priceUpperBound;
        this.unit = unit;
    }

    public int getPriceLowerBound() {
        return priceLowerBound;
    }

    public void setPriceLowerBound(int priceLowerBound) {
        this.priceLowerBound = priceLowerBound;
    }

    public int getPriceUpperBound() {
        return priceUpperBound;
    }

    public void setPriceUpperBound(int priceUpperBound) {
        this.priceUpperBound = priceUpperBound;
    }

    public double getUnit() {
        return unit;
    }

    public void setUnit(double unit) {
        this.unit = unit;
    }
}

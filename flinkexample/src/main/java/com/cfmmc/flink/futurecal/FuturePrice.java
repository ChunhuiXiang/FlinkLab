package com.cfmmc.flink.futurecal;

/**
 * Created  on 2020/2/23.
 */
public class FuturePrice {
    private String futureVariety;  // 期货类型
    private long timestampSecond;
    private double price;

    public String getFutureVariety() {
        return futureVariety;
    }

    public void setFutureVariety(String futureVariety) {
        this.futureVariety = futureVariety;
    }

    public long getTimestampSecond() {
        return timestampSecond;
    }

    public void setTimestampSecond(long timestampSecond) {
        this.timestampSecond = timestampSecond;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "FuturePrice{" +
                "futureVariety='" + futureVariety + '\'' +
                ", timestampSecond=" + timestampSecond +
                ", price=" + price +
                '}';
    }
}

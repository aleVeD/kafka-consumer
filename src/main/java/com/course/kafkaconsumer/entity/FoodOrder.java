package com.course.kafkaconsumer.entity;

public class FoodOrder {
    private int amount;
    private String item;

    public FoodOrder() {
    }

    @Override
    public String toString() {
        return "FoodOrder{" +
                "amount=" + amount +
                ", item='" + item + '\'' +
                '}';
    }

    public FoodOrder(int amount, String item) {
        this.amount = amount;
        this.item = item;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }
}

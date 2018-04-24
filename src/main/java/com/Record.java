package com;

public class Record {
    private int key;
    private String inn_1;
    private String kpp_1;
    private String inn_2;
    private String kpp_2;
    private double money;
    private double tax;


    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getInn_1() {
        return inn_1;
    }

    public void setInn_1(String inn_1) {
        this.inn_1 = inn_1;
    }

    public String getKpp_1() {
        return kpp_1;
    }

    public void setKpp_1(String kpp_1) {
        this.kpp_1 = kpp_1;
    }

    public String getInn_2() {
        return inn_2;
    }

    public void setInn_2(String inn_2) {
        this.inn_2 = inn_2;
    }

    public String getKpp_2() {
        return kpp_2;
    }

    public void setKpp_2(String kpp_2) {
        this.kpp_2 = kpp_2;
    }

    public double getMoney() {
        return money;
    }

    public void setMoney(double money) {
        this.money = money;
    }

    public double getTax() {
        return tax;
    }

    public void setTax(double tax) {
        this.tax = tax;
    }
}

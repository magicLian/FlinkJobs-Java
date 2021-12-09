package com.adv.models;

public class RandomIntOutput {
    private Integer ret;
    private Long ts;

    public RandomIntOutput(Integer ret, Long t) {
        this.ret = ret;
        this.ts = t;
    }

    public Integer getRet() {
        return ret;
    }

    public void setRet(Integer ret) {
        this.ret = ret;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "RandomIntOutput{" +
                "ret=" + ret +
                ", ts=" + ts +
                '}';
    }
}

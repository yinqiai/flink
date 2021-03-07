package com.transsnet.study.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class SourceEvent {
    private String name;
    private Integer age;

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static SourceEvent fromString(String s) {

        JSONObject jsonObject = JSON.parseObject(s);
        String name = jsonObject.getString("name");
        Integer age = jsonObject.getInteger("age");
        SourceEvent sourceEvent = new SourceEvent();
        sourceEvent.setAge(age);
        sourceEvent.setName(name);
        return sourceEvent;
    }
}

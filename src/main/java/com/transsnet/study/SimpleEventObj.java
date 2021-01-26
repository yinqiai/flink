package com.transsnet.study;

import java.io.Serializable;

/**
 * @author yinqi
 * @date 2020/12/24
 */
public class SimpleEventObj implements Serializable {
    private Long datetime;
    private String name;

    public SimpleEventObj(Long datetime, String name) {
        this.datetime = datetime;
        this.name = name;
    }

    public Long getDatetime() {
        return datetime;
    }

    public void setDatetime(Long datetime) {
        this.datetime = datetime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

package com.transsnet.study.study20210223;

import java.lang.reflect.Field;

public class ChannelInfo {
    private String id;
    private String payId;
    private String subPayId;
    private int payType;
    private String payChannel;
    private int status;
    private Long amount;
    private Long createTime;
    private Long updateTime;
    private String channelBusiness;
    private String network;
    private String bankCode;
    private String networkPayType;
    private String bankCodePayType;

    public ChannelInfo(String id, String payId, String subPayId, int payType, String payChannel, int status, Long amount, Long createTime, Long updateTime, String network, String bankCode) {
        this.id = id;
        this.payId = payId;
        this.subPayId = subPayId;
        this.payType = payType;
        this.payChannel = payChannel;
        this.status = status;
        this.amount = amount;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.network = network;
        this.bankCode = bankCode;
        this.channelBusiness = payChannel + "\001" + payType;
        this.networkPayType = network + "\001" + payType;
        this.bankCodePayType = bankCode + "\001" + payType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPayId() {
        return payId;
    }

    public void setPayId(String payId) {
        this.payId = payId;
    }

    public String getSubPayId() {
        return subPayId;
    }

    public void setSubPayId(String subPayId) {
        this.subPayId = subPayId;
    }

    public int getPayType() {
        return payType;
    }

    public void setPayType(int payType) {
        this.payType = payType;
    }

    public String getPayChannel() {
        return payChannel;
    }

    public void setPayChannel(String payChannel) {
        this.payChannel = payChannel;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getBankCode() {
        return bankCode;
    }

    public void setBankCode(String bankCode) {
        this.bankCode = bankCode;
    }

    public String getChannelBusiness() {
        return channelBusiness;
    }

    public String getNetworkPayType() {
        return networkPayType;
    }

    public String getBankCodePayType() {
        return bankCodePayType;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder("[");
        for (Field declaredField : ChannelInfo.class.getDeclaredFields()) {
            try {
                result
                        .append(declaredField.getName())
                        .append("=")
                        .append(declaredField.get(ChannelInfo.this))
                        .append(",");
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return result.substring(0, result.length() - 1) + "]";
    }
}

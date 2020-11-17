package spring.boot.common.zookeeper.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author : yanwenhui
 * @description :
 * @date : 2020/8/21
 */
@ConfigurationProperties(prefix = "spring.zookeeper")
public class ZookeeperProperties {

    /**
     * 主机地址
     */
    private String host;

    /**
     * exponential-backoff-retry: 重试指定的次数, 且每一次重试之间停顿的时间逐渐增加
     * retry-n-times: 指定最大重试次数的重试策略
     * retry-one-time: 只重试一次
     * retry-forever: 永远重试
     * retry-until-elapsed: 一直重试直到达到规定的时间
     */
    private String retryPolicy = "retry-until-elapsed";

    /**
     * session 过期时间
     */
    private int sessionTimeOut = 5000;

    /**
     * connect 过期时间
     */
    private int connectTimeOut = 5000;

    /**
     * 命名空间
     */
    private String nameSpace;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getRetryPolicy() {
        return retryPolicy;
    }

    public void setRetryPolicy(String retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public int getSessionTimeOut() {
        return sessionTimeOut;
    }

    public void setSessionTimeOut(int sessionTimeOut) {
        this.sessionTimeOut = sessionTimeOut;
    }

    public int getConnectTimeOut() {
        return connectTimeOut;
    }

    public void setConnectTimeOut(int connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }
}

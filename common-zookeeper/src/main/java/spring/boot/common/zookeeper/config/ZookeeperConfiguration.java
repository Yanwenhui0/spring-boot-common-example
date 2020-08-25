package spring.boot.common.zookeeper.config;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author : yanwenhui
 * @description :
 * @date : 2020/8/21
 */
@Configuration
@ConditionalOnMissingBean({CuratorFramework.class})
@EnableConfigurationProperties(ZookeeperProperties.class)
public class ZookeeperConfiguration {

    private final ZookeeperProperties zookeeperProperties;

    public ZookeeperConfiguration(ZookeeperProperties zookeeperProperties) {
        this.zookeeperProperties = zookeeperProperties;
    }

    @Bean
    public CuratorFramework curatorTemplate() {
        RetryPolicy retryPolicy;
        switch (zookeeperProperties.getRetryPolicy()) {
            case "exponential-backoff-retry":
                // 重试指定的次数, 且每一次重试之间停顿的时间逐渐增加
                retryPolicy = new ExponentialBackoffRetry(1000, 5, 5000);
                break;
            case "retry-n-times":
                // 指定最大重试次数的重试策略
                retryPolicy = new RetryNTimes(5, 1000);
                break;
            case "retry-one-time":
                // 只重试一次
                retryPolicy = new RetryOneTime(3000);
                break;
            case "retry-forever":
                // 永远重试
                retryPolicy = new RetryForever(1000);
                break;
            case "retry-until-elapsed":
            default:
                // 一直重试直到达到规定的时间
                retryPolicy = new RetryUntilElapsed(5000, 1000);
        }
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zookeeperProperties.getHost())
                .sessionTimeoutMs(zookeeperProperties.getSessionTimeOut())
                .connectionTimeoutMs(zookeeperProperties.getConnectTimeOut())
                .retryPolicy(retryPolicy)
                .namespace(zookeeperProperties.getNameSpace())
                .build();
        curatorFramework.start();
        return curatorFramework;
    }

}

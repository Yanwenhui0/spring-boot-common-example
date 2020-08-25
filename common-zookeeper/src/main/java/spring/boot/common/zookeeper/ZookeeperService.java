package spring.boot.common.zookeeper;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.Collections;
import java.util.List;

/**
 * @author : yanwenhui
 * @description :
 * @date : 2020/8/25
 */
public class ZookeeperService {

    private CuratorFramework curatorTemplate;

    public ZookeeperService(CuratorFramework curatorTemplate) {
        this.curatorTemplate = curatorTemplate;
    }

    public void releaseLock(String key) throws Exception {
        curatorTemplate.delete().guaranteed().forPath(key);
    }

    public boolean lock(String key) throws Exception {
        // 设置获取锁的超时时间
        long getLockTime = System.currentTimeMillis() + 60 * 1000;

        boolean isLock = false;
        // 循环获取锁，持续时间 3 * 1000 ms
        while (System.currentTimeMillis() < getLockTime) {

            //
            String status = tryLock(key.substring(1));
            if(StringUtils.equals("true", status)) {
                isLock = true;
                break;
            }
            waitForLock(status);
        }
        return isLock;
    }

    private String tryLock(String key) throws Exception {
        List<String> childList = curatorTemplate.getChildren().forPath("/");
        Collections.sort(childList);
        String firstNode;
        if(CollectionUtils.isEmpty(childList) || StringUtils.isBlank(firstNode = childList.get(0))) {
            throw new RuntimeException("zookeeper中不存在node!");
        }

        // 以获取到锁
        if(StringUtils.equals(key, firstNode)) {
            return "true";
        }

        for (int i = 0; i < childList.size(); i++) {
            if(StringUtils.equals(key, childList.get(i))) {
                return childList.get(i - 1);
            }
        }

        throw new RuntimeException("zookeeper中不存在node!");
    }

    private void waitForLock(String brother) throws Exception {
        // 监听 brother 节点
        Stat stat;

        while (true) {
            System.out.println("等待释放 :: /" + brother);
            stat = curatorTemplate.checkExists().forPath("/" + brother);
            if(ObjectUtils.isEmpty(stat)) {
                return;
            }
            Thread.sleep(50);
        }
    }

}

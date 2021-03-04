package com.register.client.edu;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 服务注册中心的客户端缓存的一个服务注册表
 */
public class CachedServiceRegistry {

    /**
     * 服务注册表拉取间隔时间
     */
    private static final Long SERVICE_REGISTRY_FETCH_INTERVAL = 30 * 1000L;

    private AtomicReference<Applications> applications =
            new AtomicReference<Applications>(new Applications());

    /**
     * 负责定时拉取注册表到客户端进行缓存的后台线程
     */
    private FetchDeltaRegistryWorker fetchDeltaRegistryWorker;

    /**
     * RegisteClient
     */
    private RegisterClient registerClient;

    /**
     * http通信组件
     */
    private HttpSender httpSender;

    /**
     * 构造函数
     */
    public CachedServiceRegistry(RegisterClient registerClient,
                                 HttpSender httpSender) {
        this.fetchDeltaRegistryWorker = new FetchDeltaRegistryWorker();
        this.registerClient = registerClient;
        this.httpSender = httpSender;
    }

    /**
     * 初始化
     */
    public void initialize(){
        // 启动全量拉取注册表的线程
        FetchFullRegistryWorker fetchFullRegistryWorker =
                new FetchFullRegistryWorker();
        fetchFullRegistryWorker.start();
        this.fetchDeltaRegistryWorker.start();
    }

    /**
     * 销毁这个组件
     */
    public void destroy(){
        this.fetchDeltaRegistryWorker.interrupt();
    }

    /**
     * 全量拉取注册表的后台线程
     */
    private class FetchFullRegistryWorker extends Thread {
        @Override
        public void run() {
            Applications fetchedApplications = httpSender.fetchFullRegistry();
            while (true) {
                Applications expectedApplications = CachedServiceRegistry.this.applications.get();
                if (applications.compareAndSet(expectedApplications,fetchedApplications)) {
                    break;
                }
            }
        }
    }

    /**
     * 增量拉取注册表的后台线程
     */
    private class FetchDeltaRegistryWorker extends Thread{
        @Override
        public void run() {
            while (registerClient.isRunning()) {
                try {
                    Thread.sleep(SERVICE_REGISTRY_FETCH_INTERVAL);

                    // 拉取回来最近三分钟变化的服务实例
                    DeltaRegistry deltaRegistry = httpSender.fetchDeltaRegistry();

                    // 合并操作
                    mergeDeltaRegistry(deltaRegistry);
                    // 校对调整注册表
                    reconcileRegistry(deltaRegistry);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * 合并增量注册表到本地缓存注册表里去
         * @param deltaRegistry
         */
        private void mergeDeltaRegistry(DeltaRegistry deltaRegistry) {
            synchronized (applications) {
                Map<String,Map<String,ServiceInstance>> registry =
                        applications.get().getRegistry();

                LinkedList<RecentlyChangedServiceInstance> recentlyChangedQueue =
                        deltaRegistry.getRecentlyChangedQueue();

                for (RecentlyChangedServiceInstance recentlyChangedItem : recentlyChangedQueue) {
                    String serviceName = recentlyChangedItem.serviceInstance.getServiceName();
                    String serviceInstanceId = recentlyChangedItem.serviceInstance.getServiceInstanceId();

                    // 如果是注册操作
                    if (ServiceInstanceOperation.REGISTER.equals(
                            recentlyChangedItem.serviceInstanceOperation)) {
                        Map<String, ServiceInstance> serviceInstanceMap = registry.get(serviceName);
                        if (serviceInstanceMap == null) {
                            serviceInstanceMap = new HashMap<String, ServiceInstance>();
                            registry.put(serviceName,serviceInstanceMap);
                        }

                        ServiceInstance serviceInstance = serviceInstanceMap.get(serviceInstanceId);
                        if (serviceInstance == null) {
                            serviceInstanceMap.put(serviceInstanceId,recentlyChangedItem.serviceInstance);
                        }
                    }

                    //如果是删除操作
                    else if (ServiceInstanceOperation.REMOVE.equals(
                            recentlyChangedItem.serviceInstanceOperation)) {
                        Map<String, ServiceInstance> serviceInstanceMap = registry.get(serviceName);
                        if (serviceInstanceMap != null) {
                            serviceInstanceMap.remove(serviceInstanceId);
                        }
                    }
                }
            }
        }

        /**
         * 校对调整注册表
         * @param deltaRegistry
         */
        private void reconcileRegistry(DeltaRegistry deltaRegistry){
            Map<String, Map<String, ServiceInstance>> registry = applications.get().getRegistry();
            Long serverSideTotalCount = deltaRegistry.getServiceInstanceTotalCount();
            Long clientSideTotalCount = 0L;
            for (Map<String, ServiceInstance> serviceInstanceMap : registry.values()) {
                clientSideTotalCount += serviceInstanceMap.size();
            }

            if (serverSideTotalCount != clientSideTotalCount) {
                // 重新拉取全量注册表进行纠正
                Applications fetchedApplications = httpSender.fetchFullRegistry();
                while (true) {
                    Applications expectedApplications = applications.get();
                    if (applications.compareAndSet(expectedApplications,fetchedApplications)) {
                        break;
                    }
                }
            }
        }
    }

    /**
     * 获取服务注册表
     * @return
     */
    public Map<String,Map<String,ServiceInstance>> getRegistry(){
        return applications.get().getRegistry();
    }

    /**
     * 服务实例操作
     */
    class ServiceInstanceOperation{
        /**
         * 注册
         */
        public static final String REGISTER = "register";
        /**
         * 删除
         */
        public static final String REMOVE = "REMOVE";
    }

    /**
     * 最近变更的实例信息
     */
    static class RecentlyChangedServiceInstance{
        /**
         * 服务实例
         */
        ServiceInstance serviceInstance;
        /**
         * 发生变更的时间戳
         */
        Long changedTimestamp;
        /**
         * 变更操作
         */
        String serviceInstanceOperation;

        public RecentlyChangedServiceInstance(
                ServiceInstance serviceInstance,
                Long changedTimestamp,
                String serviceInstanceOperation) {
            this.serviceInstance = serviceInstance;
            this.changedTimestamp = changedTimestamp;
            this.serviceInstanceOperation = serviceInstanceOperation;
        }

        @Override
        public String toString() {
            return "RecentlyChangedServiceInstance [serviceInstance=" + serviceInstance + ", changedTimestamp="
                    + changedTimestamp + ", serviceInstanceOperation=" + serviceInstanceOperation + "]";
        }
    }

}

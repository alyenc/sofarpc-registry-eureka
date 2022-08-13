package dev.kaifa.sofa.rpc.registry.oss;

import com.alipay.sofa.rpc.client.ProviderGroup;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.common.struct.MapDifference;
import com.alipay.sofa.rpc.common.struct.ScheduledService;
import com.alipay.sofa.rpc.common.struct.ValueDifference;
import com.alipay.sofa.rpc.common.utils.CommonUtils;
import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.RegistryConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.event.ConsumerSubEvent;
import com.alipay.sofa.rpc.event.EventBus;
import com.alipay.sofa.rpc.event.ProviderPubEvent;
import com.alipay.sofa.rpc.ext.Extension;
import com.alipay.sofa.rpc.listener.ProviderInfoListener;
import com.alipay.sofa.rpc.log.LogCodes;
import com.alipay.sofa.rpc.log.Logger;
import com.alipay.sofa.rpc.log.LoggerFactory;
import com.alipay.sofa.rpc.registry.Registry;
import com.aliyun.oss.*;
import com.aliyun.oss.model.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * sofarpc
 * OSS注册中心
 * 前置准备：
 *      新建bucket
 *      开启版本控制
 */
@Extension("oss")
public class OssRegistry extends Registry {

    public static final String EXT_NAME = "OssRegistry";

    /**
     * slf4j Logger for this class
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(OssRegistry.class);

    /**
     * 定时加载
     */
    private ScheduledService scheduledExecutorService;

    /**
     * 内存里的服务列表 {service : [provider...]}
     */
    protected Map<String, ProviderGroup> memoryCache = new ConcurrentHashMap<String, ProviderGroup>();

    /**
     * 内存发生了变化，如果为true，则将触发写OSS动作
     */
    private boolean needBackup = false;

    /**
     * 是否订阅通知（即扫描文件变化），默认为true
     * 如果FileRegistry是被动加载（例如作为注册中心备份的）的，建议false，防止重复通知
     */
    private boolean subscribe = true;

    /**
     * 订阅者通知列表（key为订阅者关键字，value为ConsumerConfig列表）
     */
    protected Map<String, List<ConsumerConfig>> notifyListeners = new ConcurrentHashMap<String, List<ConsumerConfig>>();

    /**
     * 最新的versionId
     * OSS需要开启版本控制
     */
    private String latestVersionId;

    /**
     * 扫描周期，毫秒
     */
    private int scanPeriod = 2000;
    /**
     * 输出和备份文件目录
     */
    private String objectName;
    /**
     * 存储桶
     */
    private String bucketName;

    /**
     * OSS客户端
     */
    private OSS ossClient;

    protected OssRegistry(RegistryConfig registryConfig) {
        super(registryConfig);
    }

    @Override
    public void init() {
        if (ossClient != null) {
            return;
        }

        //OSS配置
        String endpoint = registryConfig.getAddress();
        String accessKeyId = registryConfig.getParameter("accessKeyId");
        String accessKeySecret = registryConfig.getParameter("accessKeySecret");

        // 填写Bucket名称，例如examplebucket。
        bucketName = registryConfig.getParameter("bucketName");
        objectName = registryConfig.getParameter("objectName");

        // 创建OSSClient实例。
        ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);

        // 先加载一些
        if (subscribe) {
            doLoadCache();
        }

        // 开始扫描
        this.scanPeriod = CommonUtils.parseInt(registryConfig.getParameter("registry.oss.scan.period"), scanPeriod);

        Runnable task = () -> {
            try {
                // 如果要求备份，那么说明内存中为最新的，无需加载
                doWriteFile();

                // 订阅变化（默认是不订阅的）
                // 检查摘要，如果有有变，则自动重新加载
                if (subscribe && checkModified()) {
                    doLoadCache();
                }
            } catch (Throwable e) {
                LOGGER.error(e.getMessage(), e);
            }
        };

        //启动扫描线程
        scheduledExecutorService = new ScheduledService("OssRegistry-Back-Load",
                ScheduledService.MODE_FIXEDDELAY,
                task, //定时load任务
                scanPeriod, // 延迟一个周期
                scanPeriod, // 一个周期循环
                TimeUnit.MILLISECONDS).start();

    }

    @Override
    public boolean start() {
        return false;
    }

    @Override
    public void register(ProviderConfig config) {
        String appName = config.getAppName();
        if (!registryConfig.isRegister()) {
            if (LOGGER.isInfoEnabled(appName)) {
                LOGGER.infoWithApp(appName, LogCodes.getLog(LogCodes.INFO_REGISTRY_IGNORE));
            }
            return;
        }
        if (!config.isRegister()) { // 注册中心不注册或者服务不注册
            return;
        }

        List<ServerConfig> serverConfigs = config.getServer();
        if (CommonUtils.isNotEmpty(serverConfigs)) {
            for (ServerConfig server : serverConfigs) {
                String serviceName = OssRegistryHelper.buildListDataId(config, server.getProtocol());
                ProviderInfo providerInfo = OssRegistryHelper.convertProviderToProviderInfo(config, server);
                if (LOGGER.isInfoEnabled(appName)) {
                    LOGGER.infoWithApp(appName, LogCodes.getLog(LogCodes.INFO_ROUTE_REGISTRY_PUB_START, serviceName));
                }
                doRegister(appName, serviceName, providerInfo);

                if (LOGGER.isInfoEnabled(appName)) {
                    LOGGER.infoWithApp(appName, LogCodes.getLog(LogCodes.INFO_ROUTE_REGISTRY_PUB_OVER, serviceName));
                }
            }
            if (EventBus.isEnable(ProviderPubEvent.class)) {
                ProviderPubEvent event = new ProviderPubEvent(config);
                EventBus.post(event);
            }
        }
    }

    @Override
    public void unRegister(ProviderConfig config) {
        String appName = config.getAppName();
        if (!registryConfig.isRegister()) { // 注册中心不注册
            if (LOGGER.isInfoEnabled(appName)) {
                LOGGER.infoWithApp(appName, LogCodes.getLog(LogCodes.INFO_REGISTRY_IGNORE));
            }
            return;
        }
        if (!config.isRegister()) { // 服务不注册
            return;
        }
        List<ServerConfig> serverConfigs = config.getServer();
        if (CommonUtils.isNotEmpty(serverConfigs)) {
            for (ServerConfig server : serverConfigs) {
                String serviceName = OssRegistryHelper.buildListDataId(config, server.getProtocol());
                ProviderInfo providerInfo = OssRegistryHelper.convertProviderToProviderInfo(config, server);
                try {
                    doUnRegister(serviceName, providerInfo);
                    if (LOGGER.isInfoEnabled(appName)) {
                        LOGGER.infoWithApp(appName,
                                LogCodes.getLog(LogCodes.INFO_ROUTE_REGISTRY_UNPUB, serviceName, "1"));
                    }
                } catch (Exception e) {
                    LOGGER.errorWithApp(appName, LogCodes.getLog(LogCodes.INFO_ROUTE_REGISTRY_UNPUB, serviceName, "0"), e);
                }
            }
        }
    }

    @Override
    public void batchUnRegister(List<ProviderConfig> configs) {
        for (ProviderConfig config : configs) {
            String appName = config.getAppName();
            try {
                unRegister(config);
            } catch (Exception e) {
                LOGGER.errorWithApp(appName, "Error when batch unregistry", e);
            }
        }
    }

    @Override
    public List<ProviderGroup> subscribe(ConsumerConfig config) {
        String key = OssRegistryHelper.buildListDataId(config, config.getProtocol());
        List<ConsumerConfig> listeners = notifyListeners.get(key);
        if (listeners == null) {
            listeners = new ArrayList<ConsumerConfig>();
            notifyListeners.put(key, listeners);
        }
        listeners.add(config);
        // 返回已经加载到内存的列表（可能不是最新的)
        ProviderGroup group = memoryCache.get(key);
        if (group == null) {
            group = new ProviderGroup();
            memoryCache.put(key, group);
        }

        if (EventBus.isEnable(ConsumerSubEvent.class)) {
            ConsumerSubEvent event = new ConsumerSubEvent(config);
            EventBus.post(event);
        }

        return Collections.singletonList(group);
    }

    @Override
    public void unSubscribe(ConsumerConfig config) {
        String key = OssRegistryHelper.buildListDataId(config, config.getProtocol());
        // 取消注册订阅关系，监听文件修改变化
        List<ConsumerConfig> listeners = notifyListeners.get(key);
        if (listeners != null) {
            listeners.remove(config);
            if (listeners.size() == 0) {
                notifyListeners.remove(key);
            }
        }
    }

    @Override
    public void batchUnSubscribe(List<ConsumerConfig> configs) {
        // 不支持批量反注册，那就一个个来吧
        for (ConsumerConfig config : configs) {
            String appName = config.getAppName();
            try {
                unSubscribe(config);
            } catch (Exception e) {
                LOGGER.errorWithApp(appName, "Error when batch unSubscribe", e);
            }
        }
    }

    @Override
    public void destroy() {
        try {
            //关闭OSS
            ossClient.shutdown();

            //关闭线程
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
                scheduledExecutorService = null;
            }
        } catch (Throwable t) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(t.getMessage(), t);
            }
        }
    }

    private void doLoadCache() {
        // 从OSS加载到内存
        Map<String, ProviderGroup> tempCache = loadBackupFileToCache();

        // 比较旧列表和新列表，通知订阅者变化部分
        notifyConsumer(tempCache);

        // 通知完保存到内存
        memoryCache = tempCache;
    }

    /**
     * 写文件
     */
    private void doWriteFile() {
        if (needBackup) {
            // 得到备份的文件内容
            String content = StringUtils.defaultString(OssRegistryHelper.marshalCache(memoryCache));
            PutObjectResult result = ossClient.putObject(bucketName, objectName, new ByteArrayInputStream(content.getBytes()));
            if (result != null) {
                this.latestVersionId = result.getVersionId();
                needBackup = false;
            }
        }
    }

    /**
     * 注册单条服务信息
     *
     * @param appName      应用名
     * @param serviceName  服务关键字
     * @param providerInfo 服务提供者数据
     */
    private void doRegister(String appName, String serviceName, ProviderInfo providerInfo) {
        if (LOGGER.isInfoEnabled(appName)) {
            LOGGER.infoWithApp(appName, LogCodes.getLog(LogCodes.INFO_ROUTE_REGISTRY_PUB, serviceName));
        }
        //{service : [provider...]}
        ProviderGroup oldGroup = memoryCache.get(serviceName);
        if (oldGroup != null) { // 存在老的key
            oldGroup.add(providerInfo);
        } else { // 没有老的key，第一次加入
            List<ProviderInfo> news = new ArrayList<ProviderInfo>();
            news.add(providerInfo);
            memoryCache.put(serviceName, new ProviderGroup(news));
        }
        // 备份到文件 改为定时写
        needBackup = true;
        doWriteFile();

        if (subscribe) {
            notifyConsumerListeners(serviceName, memoryCache.get(serviceName));
        }
    }

    /**
     * 反注册服务信息
     *
     * @param serviceName  服务关键字
     * @param providerInfo 服务提供者数据
     */
    private void doUnRegister(String serviceName, ProviderInfo providerInfo) {
        //{service : [provider...]}
        ProviderGroup oldGroup = memoryCache.get(serviceName);
        if (oldGroup != null) { // 存在老的key
            oldGroup.remove(providerInfo);
        } else {
            return;
        }
        // 备份到文件 改为定时写
        needBackup = true;
        doWriteFile();

        if (subscribe) {
            notifyConsumerListeners(serviceName, memoryCache.get(serviceName));
        }
    }

    /**
     * Notify consumer.
     *
     * @param newCache the new cache
     */
    private void notifyConsumer(Map<String, ProviderGroup> newCache) {
        Map<String, ProviderGroup> oldCache = memoryCache;
        // 比较两个map的差异
        MapDifference<String, ProviderGroup> difference =
                new MapDifference<String, ProviderGroup>(newCache, oldCache);
        // 新的有，旧的没有，通知
        Map<String, ProviderGroup> onlynew = difference.entriesOnlyOnLeft();
        for (Map.Entry<String, ProviderGroup> entry : onlynew.entrySet()) {
            notifyConsumerListeners(entry.getKey(), entry.getValue());
        }
        // 旧的有，新的没有，全部干掉
        Map<String, ProviderGroup> onlyold = difference.entriesOnlyOnRight();
        for (Map.Entry<String, ProviderGroup> entry : onlyold.entrySet()) {
            notifyConsumerListeners(entry.getKey(), new ProviderGroup());
        }

        // 新旧都有，而且有变化
        Map<String, ValueDifference<ProviderGroup>> changed = difference.entriesDiffering();
        for (Map.Entry<String, ValueDifference<ProviderGroup>> entry : changed.entrySet()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} has differente", entry.getKey());
            }
            ValueDifference<ProviderGroup> differentValue = entry.getValue();
            ProviderGroup innew = differentValue.leftValue();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("new(right) is {}", innew);
            }
            // 只通知变化部分内容
            notifyConsumerListeners(entry.getKey(), innew);
        }
    }

    private void notifyConsumerListeners(String serviceName, ProviderGroup providerGroup) {
        List<ConsumerConfig> consumerConfigs = notifyListeners.get(serviceName);
        if (consumerConfigs != null) {
            for (ConsumerConfig config : consumerConfigs) {
                ProviderInfoListener listener = config.getProviderInfoListener();
                if (listener != null) {
                    listener.updateProviders(providerGroup); // 更新分组
                }
            }
        }
    }

    private Map<String, ProviderGroup> loadBackupFileToCache() {
        Map<String, ProviderGroup> memoryCache = new ConcurrentHashMap<String, ProviderGroup>();
        try {
            boolean found = ossClient.doesObjectExist(bucketName, objectName);
            if(!found) {
                return memoryCache;
            }

            // ossObject包含文件所在的存储空间名称、文件名称、文件元信息以及一个输入流。
            OSSObject ossObject = ossClient.getObject(bucketName, objectName);

            // 读取文件内容。
            BufferedReader reader = new BufferedReader(new InputStreamReader(ossObject.getObjectContent()));
            StringBuffer buffer = new StringBuffer();
            String line = "";
            while ((line = reader.readLine()) != null){
                buffer.append(line);
            }

            Map<String, ProviderGroup> tmp = OssRegistryHelper.unMarshal(buffer.toString());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Load backup file from {}", this.bucketName);
            }
            // 加载到内存中
            if (tmp != null) {
                memoryCache.putAll(tmp);
            }

            // 数据读取完成后，获取的流必须关闭，否则会造成连接泄漏，导致请求无连接可用，程序无法正常工作。
            reader.close();
            // ossObject对象使用完毕后必须关闭，否则会造成连接泄漏，导致请求无连接可用，程序无法正常工作。
            ossObject.close();
        } catch (OSSException oe) {
            LOGGER.info("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            LOGGER.info("Error Message:" + oe.getErrorMessage());
            LOGGER.info("Error Code:" + oe.getErrorCode());
            LOGGER.info("Request ID:" + oe.getRequestId());
            LOGGER.info("Host ID:" + oe.getHostId());
        } catch (Throwable ce) {
            LOGGER.info("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            LOGGER.info("Error Message:" + ce.getMessage());
        } finally {
            if (ossClient != null) {
                ossClient.shutdown();
            }
        }

        return memoryCache;
    }

    private boolean checkModified() {
        VersionListing versionListing = ossClient.listVersions(this.bucketName, this.objectName);
        List<OSSVersionSummary> versionSummaryList = versionListing.getVersionSummaries();
        for(OSSVersionSummary item: versionSummaryList) {
            boolean isLatest = item.isLatest();
            if(isLatest) {
                return StringUtils.equals(this.latestVersionId, item.getVersionId());
            }
        }

        return false;
    }
}

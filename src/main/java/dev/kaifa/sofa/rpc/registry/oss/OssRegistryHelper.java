package dev.kaifa.sofa.rpc.registry.oss;

import com.alipay.sofa.rpc.client.ProviderGroup;
import com.alipay.sofa.rpc.client.ProviderHelper;
import com.alipay.sofa.rpc.client.ProviderInfo;
import com.alipay.sofa.rpc.client.ProviderInfoAttrs;
import com.alipay.sofa.rpc.common.RpcConstants;
import com.alipay.sofa.rpc.common.SystemInfo;
import com.alipay.sofa.rpc.common.utils.CommonUtils;
import com.alipay.sofa.rpc.common.utils.FileUtils;
import com.alipay.sofa.rpc.common.utils.NetUtils;
import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.alipay.sofa.rpc.config.AbstractInterfaceConfig;
import com.alipay.sofa.rpc.config.ConfigUniqueNameGenerator;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.context.RpcRuntimeContext;
import com.alipay.sofa.rpc.log.Logger;
import com.alipay.sofa.rpc.log.LoggerFactory;
import com.aliyun.oss.OSS;
;
import java.util.*;

/**
 *
 */
public class OssRegistryHelper {
    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(OssRegistryHelper.class);

    private static String SEPARATORSTR = "\t";

    /**
     * 转为服务端提供者对象
     *
     * @param config 服务提供者配置
     * @param server 服务端
     * @return 本地服务提供者对象
     */
    public static ProviderInfo convertProviderToProviderInfo(ProviderConfig config, ServerConfig server) {
        ProviderInfo providerInfo = new ProviderInfo()
                .setPort(server.getPort())
                .setWeight(config.getWeight())
                .setSerializationType(config.getSerialization())
                .setProtocolType(server.getProtocol())
                .setPath(server.getContextPath())
                .setStaticAttrs(config.getParameters());
        String host = server.getHost();
        if (NetUtils.isLocalHost(host) || NetUtils.isAnyHost(host)) {
            host = SystemInfo.getLocalHost();
        }
        providerInfo.setHost(host);
        return providerInfo;
    }

    public static synchronized String marshalCache(Map<String, ProviderGroup> memoryCache) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ProviderGroup> entry : memoryCache.entrySet()) {
            ProviderGroup group = entry.getValue();
            if (group != null) {
                List<ProviderInfo> ps = group.getProviderInfos();
                if (CommonUtils.isNotEmpty(ps)) {
                    sb.append(entry.getKey()).append(SEPARATORSTR);
                    for (ProviderInfo providerInfo : ps) {
                        sb.append(ProviderHelper.toUrl(providerInfo)).append(SEPARATORSTR);
                    }
                    sb.append(FileUtils.LINE_SEPARATOR);
                }
            }
        }
        return sb.toString();
    }

    public static synchronized Map<String, ProviderGroup> unMarshal(String context) {
        if (StringUtils.isBlank(context)) {
            return null;
        }
        Map<String, ProviderGroup> map = new HashMap<String, ProviderGroup>();
        String[] lines = StringUtils.split(context, FileUtils.LINE_SEPARATOR);
        for (String line : lines) {
            String[] fields = line.split(SEPARATORSTR);
            if (fields.length > 1) {
                String key = fields[0];
                Set<ProviderInfo> values = new HashSet<ProviderInfo>();
                for (int i = 1; i < fields.length; i++) {
                    String pstr = fields[i];
                    if (StringUtils.isNotEmpty(pstr)) {
                        ProviderInfo providerInfo = ProviderHelper.toProviderInfo(pstr);
                        providerInfo.setStaticAttr(ProviderInfoAttrs.ATTR_SOURCE, "oss");
                        values.add(providerInfo);
                    }
                }
                map.put(key, new ProviderGroup(new ArrayList<ProviderInfo>(values)));
            }
        }
        return map;
    }

    /**
     * 服务注册中心的Key
     *
     * @param config   配置
     * @param protocol 协议
     * @return 返回值
     */
    static String buildListDataId(AbstractInterfaceConfig config, String protocol) {
        if (RpcConstants.PROTOCOL_TYPE_BOLT.equals(protocol)
                || RpcConstants.PROTOCOL_TYPE_TR.equals(protocol)) {
            return ConfigUniqueNameGenerator.getUniqueName(config) + "@DEFAULT";
        } else {
            return ConfigUniqueNameGenerator.getUniqueName(config) + "@" + protocol;
        }
    }
}

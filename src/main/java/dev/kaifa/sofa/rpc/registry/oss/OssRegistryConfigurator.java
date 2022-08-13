package dev.kaifa.sofa.rpc.registry.oss;

import com.alipay.sofa.rpc.boot.common.RegistryParseUtil;
import com.alipay.sofa.rpc.boot.config.RegistryConfigureProcessor;
import com.alipay.sofa.rpc.config.RegistryConfig;

import java.util.Map;

public class OssRegistryConfigurator  implements RegistryConfigureProcessor {

    public static final String  REGISTRY_PROTOCOL_OSS = "oss";

    public OssRegistryConfigurator() {}

    @Override
    public RegistryConfig buildFromAddress(String address) {
        String sofaRegistryAddress = RegistryParseUtil.parseAddress(address, REGISTRY_PROTOCOL_OSS);
        Map<String, String> map = RegistryParseUtil.parseParam(address, REGISTRY_PROTOCOL_OSS);

        return new RegistryConfig().setAddress(sofaRegistryAddress)
                .setProtocol(REGISTRY_PROTOCOL_OSS).setParameters(map);
    }

    @Override
    public String registryType() {
        return REGISTRY_PROTOCOL_OSS;
    }
}

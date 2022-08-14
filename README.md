# 基于阿里云OSS的SOFARPC注册中心

基于SOFARPC中local注册中心修改而来，将文件存储由本地修改为oss
未经充分测试，使用需谨慎

## 引入依赖

```xml
<dependency>
    <groupId>dev.kaifa</groupId>
    <artifactId>sofarpc-registry-oss</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

## 注册中心配置

```xml
com.alipay.sofa.rpc.registry.address=oss://oss-cn-hangzhou.aliyuncs.com?accessKeyId={yourAccessKeyId}&accessKeySecret={yourAccessKeySecret}&bucketName={yourBucketName}&objectName={yourObjectName}
```

## 创建Bean

```java
@Bean
@ConditionalOnMissingBean
public OssRegistryConfigurator ossRegistryConfigurator() {
    return new OssRegistryConfigurator();
}
```

## 注意事项
- 提前创建bucket
- 确保accessKey有bucket读写权限
- bucket需要开启版本控制
- 如果生成的配置文件版本过多，可以开启过期测策略。


package io.github.majusko.pulsar;


import io.github.majusko.pulsar.properties.PulsarProperties;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.Map;

/**
 * {@link PulsarClient} 容器。
 *
 * @author MiNG
 * @since 1.0.0
 */
public interface PulsarClientContainer extends AutoCloseable {

    /**
     * 默认集群。
     */
    String DEFAULT_CLUSTER = "default";

    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarClient} 实例，
     * 如果 {@code cluster} 配置不存在则抛出 {@link IllegalArgumentException}。
     *
     * @param cluster 集群名称
     *
     * @return {@link PulsarClient} 实例，永不为空
     *
     * @throws IllegalArgumentException 对应 {@code cluster} 名称没有配置
     */
    
    PulsarClient getClient( String cluster);


    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarClient} 实例，
     * 如果 {@code cluster} 为空或对应配置不存在则获取 {@code defaultCluster} 对应的 {@link PulsarClient} 实例，
     * 如果 {@code defaultCluster} 也不存在，最后则抛出 {@link IllegalArgumentException}。
     *
     * @param cluster        集群名称
     * @param defaultCluster {@code cluster} 为空或对应配置不存在时使用的配置名称
     *
     * @return {@link PulsarClient} 实例，永不为空
     *
     * @throws IllegalArgumentException 对应 {@code cluster}/{@code defaultCluster} 名称均没有配置
     */
    
    default PulsarClient getClient( final String cluster,  final String defaultCluster) {
        final PulsarClient found = this.findClient(cluster);
        return found != null ? found : this.getClient(defaultCluster);
    }

    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarClient} 实例，
     * 如果 {@code cluster} 配置不存在则返回 {@code null}。
     *
     * @param cluster 集群名称
     *
     * @return {@link PulsarClient} 实例，在 {@code cluster} 配置不存在时候返回 {@code null}。
     *
     * @apiNote 此方法不会抛出异常
     */
    
    PulsarClient findClient( String cluster);

    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarClient} 实例，
     * 如果 {@code cluster} 为空或对应配置不存在则获取 {@code defaultCluster} 对应的 {@link PulsarClient} 实例，
     * 如果 {@code defaultCluster} 也不存在，最后则返回 {@code null}。
     *
     * @param cluster        集群名称
     * @param defaultCluster {@code cluster} 为空或对应配置不存在时使用的配置名称
     *
     * @return {@link PulsarClient} 实例，在 {@code cluster}/{@code defaultCluster} 均没有配置时候返回 {@code null}
     *
     * @apiNote 此方法不会抛出异常
     */
    
    default PulsarClient findClient( final String cluster,  final String defaultCluster) {
        final PulsarClient found = this.findClient(cluster);
        return found != null ? found : this.findClient(defaultCluster);
    }

    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarProperties}，
     * 如果 {@code cluster} 配置不存在则抛出 {@link IllegalArgumentException}。
     *
     * @param cluster 集群名称
     *
     * @return {@link PulsarProperties} 实例，永不为空
     *
     * @throws IllegalArgumentException 对应 {@code cluster} 名称没有配置
     */
    
    PulsarProperties getProperties( String cluster);

    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarProperties}，
     * 如果 {@code cluster} 为空或对应配置不存在则获取 {@code defaultCluster} 对应的 {@link PulsarProperties}，
     * 如果 {@code defaultCluster} 也不存在，最后则抛出 {@link IllegalArgumentException}。
     *
     * @param cluster        集群名称
     * @param defaultCluster {@code cluster} 为空或对应配置不存在时使用的配置名称
     *
     * @return {@link PulsarProperties} 实例，永不为空
     *
     * @throws IllegalArgumentException 对应 {@code cluster}/{@code defaultCluster} 名称均没有配置
     */
    
    default PulsarProperties getProperties( final String cluster,  final String defaultCluster) {
        final PulsarProperties found = this.findProperties(cluster);
        return found != null ? found : this.getProperties(defaultCluster);
    }

    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarProperties}，
     * 如果 {@code cluster} 配置不存在则返回 {@code null}。
     *
     * @param cluster 集群名称
     *
     * @return {@link PulsarClient} 实例，在 {@code cluster} 配置不存在时候返回 {@code null}。
     *
     * @apiNote 此方法不会抛出异常
     */
    PulsarProperties findProperties( String cluster);

    /**
     * 获取指定 {@code cluster} 对应的 {@link PulsarProperties} 实例，
     * 如果 {@code cluster} 为空或对应配置不存在则获取 {@code defaultCluster} 对应的 {@link PulsarProperties}，
     * 如果 {@code defaultCluster} 也不存在，最后则返回 {@code null}。
     *
     * @param cluster        集群名称
     * @param defaultCluster {@code cluster} 为空或对应配置不存在时使用的配置名称
     *
     * @return {@link PulsarClient} 实例，在 {@code cluster}/{@code defaultCluster} 均没有配置时候返回 {@code null}
     *
     * @apiNote 此方法不会抛出异常
     */
    default PulsarProperties findProperties( final String cluster,  final String defaultCluster) {
        final PulsarProperties found = this.findProperties(cluster);
        return found != null ? found : this.findProperties(defaultCluster);
    }

    Map<String, PulsarProperties> getAllProperties();

}

package im.xiaoyao.presto.ethereum;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import im.xiaoyao.presto.ethereum.udfs.EthereumUDFs;

import java.util.Set;

public class EthereumPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new EthereumConnectorFactory());
    }

    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.of(EthereumUDFs.class);
    }
}

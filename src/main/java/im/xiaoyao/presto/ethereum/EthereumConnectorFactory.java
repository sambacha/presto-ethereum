package im.xiaoyao.presto.ethereum;

import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class EthereumConnectorFactory implements ConnectorFactory {
    @Override
    public String getName() {
        return "ethereum";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new EthereumHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> config, ConnectorContext context) {
        requireNonNull(connectorId, "connectorId is null");
        requireNonNull(config, "config is null");

        try {
            Bootstrap app = new Bootstrap(
//                    new JsonModule(),
                    new EthereumConnectorModule(),
                    binder -> {
                        binder.bind(EthereumConnectorId.class).toInstance(new EthereumConnectorId(connectorId));
                        binder.bind(TypeManager.class).toInstance(context.getTypeManager());
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                    }
            );

            Injector injector = app.strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .initialize();

            return injector.getInstance(EthereumConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}

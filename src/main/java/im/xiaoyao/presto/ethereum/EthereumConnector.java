package im.xiaoyao.presto.ethereum;

import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import static io.prestosql.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.prestosql.spi.transaction.IsolationLevel.checkConnectorSupports;
import static java.util.Objects.requireNonNull;

public class EthereumConnector implements Connector {
    private static final Logger log = Logger.get(EthereumConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final EthereumMetadata metadata;
    private final EthereumSplitManager splitManager;
    private final EthereumRecordSetProvider recordSetProvider;

    @Inject
    public EthereumConnector(
            LifeCycleManager lifeCycleManager,
            EthereumMetadata metadata,
            EthereumSplitManager splitManager,
            EthereumRecordSetProvider recordSetProvider
    ) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        return EthereumTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error("Error shutting down connector");
        }
    }
}

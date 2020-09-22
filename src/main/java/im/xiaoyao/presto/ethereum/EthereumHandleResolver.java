package im.xiaoyao.presto.ethereum;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class EthereumHandleResolver implements ConnectorHandleResolver {
    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return EthereumTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return EthereumColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return EthereumSplit.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return EthereumTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
        return EthereumTransactionHandle.class;
    }

    static EthereumTableHandle convertTableHandle(ConnectorTableHandle tableHandle) {
        requireNonNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof EthereumTableHandle, "tableHandle is not an instance of EthereumTableHandle");
        return (EthereumTableHandle) tableHandle;
    }

    static EthereumColumnHandle convertColumnHandle(ColumnHandle columnHandle) {
        requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof EthereumColumnHandle, "columnHandle is not an instance of EthereumColumnHandle");
        return (EthereumColumnHandle) columnHandle;
    }

    static EthereumSplit convertSplit(ConnectorSplit split) {
        requireNonNull(split, "split is null");
        checkArgument(split instanceof EthereumSplit, "split is not an instance of EthereumSplit");
        return (EthereumSplit) split;
    }

    static EthereumTableLayoutHandle convertLayout(ConnectorTableLayoutHandle layout) {
        requireNonNull(layout, "layout is null");
        checkArgument(layout instanceof EthereumTableLayoutHandle, "layout is not an instance of EthereumTableLayoutHandle");
        return (EthereumTableLayoutHandle) layout;
    }
}

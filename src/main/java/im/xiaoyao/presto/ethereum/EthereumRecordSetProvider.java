package im.xiaoyao.presto.ethereum;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.web3j.protocol.Web3j;

import javax.inject.Inject;

import java.util.List;

import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertColumnHandle;
import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertSplit;

public class EthereumRecordSetProvider implements ConnectorRecordSetProvider {
    private final Web3j web3j;

    @Inject
    public EthereumRecordSetProvider(EthereumWeb3jProvider web3jProvider) {
        this.web3j = web3jProvider.getWeb3j();
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            List<? extends ColumnHandle> columns
    ) {
        EthereumSplit ethereumSplit = convertSplit(split);

        ImmutableList.Builder<EthereumColumnHandle> handleBuilder = ImmutableList.builder();

        for (ColumnHandle handle : columns) {
            EthereumColumnHandle columnHandle = convertColumnHandle(handle);
            handleBuilder.add(columnHandle);
        }

        return new EthereumRecordSet(web3j, handleBuilder.build(), ethereumSplit);
    }
}

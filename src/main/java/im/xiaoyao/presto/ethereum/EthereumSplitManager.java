package im.xiaoyao.presto.ethereum;

<<<<<<< HEAD
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
=======
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
>>>>>>> 188b23f9966f47aaf0b13155937da6e6d47641ef
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthBlockNumber;

import javax.inject.Inject;
import java.io.IOException;

import static im.xiaoyao.presto.ethereum.EthereumHandleResolver.convertLayout;
import static java.util.Objects.requireNonNull;

public class EthereumSplitManager implements ConnectorSplitManager {
    private static final Logger log = Logger.get(EthereumSplitManager.class);

    private final String connectorId;
    private final Web3j web3j;
    private final EthereumSplitSourceManager ssMgr;

    @Inject
    public EthereumSplitManager(
            EthereumConnectorId connectorId,
            EthereumConnectorConfig config,
            EthereumWeb3jProvider web3jProvider,
            EthereumSplitSourceManager ssMgr
    ) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        requireNonNull(web3jProvider, "web3j is null");
        requireNonNull(config, "config is null");
        this.ssMgr = requireNonNull(ssMgr, "ssMgr is null");
        this.web3j = web3jProvider.getWeb3j();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy
    ) {
        EthereumTableLayoutHandle tableLayoutHandle = convertLayout(layout);
        EthereumTableHandle tableHandle = tableLayoutHandle.getTable();

        try {
            EthBlockNumber blockNumber = web3j.ethBlockNumber().send();
            log.info("current block number: " + blockNumber.getBlockNumber());
            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

            for (EthereumBlockRange blockRange : tableLayoutHandle.getBlockRanges()) {
                log.info("start: %d\tend: %d", blockRange.getStartBlock(), blockRange.getEndBlock());
                for (long i = blockRange.getStartBlock(); i <= (blockRange.getEndBlock() == -1 ? blockNumber.getBlockNumber().longValue() : blockRange.getEndBlock()); i++) {
                    EthereumSplit split = new EthereumSplit(i, EthereumTable.valueOf(tableHandle.getTableName().toUpperCase()));
                    splits.add(split);
                }
            }

            if (tableLayoutHandle.getBlockRanges().isEmpty()) {
                for (long i = 1; i <= blockNumber.getBlockNumber().longValue(); i++) {
                    EthereumSplit split = new EthereumSplit(i, EthereumTable.valueOf(tableHandle.getTableName().toUpperCase()));
                    splits.add(split);
                }
            }

            ImmutableList<ConnectorSplit> connectorSplits = splits.build();
            log.info("Built %d splits", connectorSplits.size());
//            return new FixedSplitSource(connectorSplits);
            return ssMgr.put(session.getQueryId(), connectorSplits);

//            return ssMgr.get(session.getQueryId());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot get block number: ", e);
        }
    }
}

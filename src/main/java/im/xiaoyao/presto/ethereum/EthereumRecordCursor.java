package im.xiaoyao.presto.ethereum;

import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class EthereumRecordCursor implements RecordCursor {
    private static final Logger log = Logger.get(EthereumRecordCursor.class);

    private final EthBlock.Block block;
    private final Iterator<EthBlock.Block> blockIter;
    private final Iterator<EthBlock.TransactionResult> txIter;
    private final Iterator<Log> logIter;

    private final EthereumTable table;
    private final Web3j web3j;

    private final List<EthereumColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private List<Supplier> suppliers;

    public EthereumRecordCursor(List<EthereumColumnHandle> columnHandles, EthBlock.Block block, EthereumTable table) {
        this.columnHandles = columnHandles;
        this.table = table;
        this.web3j = web3j;
        this.suppliers = Collections.emptyList();

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            EthereumColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        // TODO: handle failure upstream
        this.block = requireNonNull(block, "block is null");
        this.blockIter = ImmutableList.of(block).iterator();
        this.txIter = block.getTransactions().iterator();
    }

    @Override
    public long getTotalBytes() {
        return block.getSize().longValue();
    }

    @Override
    public long getCompletedBytes() {
        return block.getSize().longValue();
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        if (table == EthereumTable.BLOCK && !blockIter.hasNext()
                || table == EthereumTable.TRANSACTION && !txIter.hasNext()
                || table == EthereumTable.ERC20 && !logIter.hasNext()) {
            return false;
        }

        ImmutableList.Builder<Supplier> builder = ImmutableList.builder();
        if (table == EthereumTable.BLOCK) {
            blockIter.next();
            builder.add(this.block::getNumber);
            builder.add(this.block::getHash);
            builder.add(this.block::getParentHash);
            builder.add(this.block::getNonceRaw);
            builder.add(this.block::getSha3Uncles);
            builder.add(this.block::getLogsBloom);
            builder.add(this.block::getTransactionsRoot);
            builder.add(this.block::getStateRoot);
            builder.add(this.block::getMiner);
            builder.add(this.block::getDifficulty);
            builder.add(this.block::getTotalDifficulty);
            builder.add(this.block::getSize);
            builder.add(this.block::getExtraData);
            builder.add(this.block::getGasLimit);
            builder.add(this.block::getGasUsed);
            builder.add(this.block::getTimestamp);
            builder.add(() -> {
                return this.block.getTransactions()
                        .stream()
                        .map(tr -> ((EthBlock.TransactionObject) tr.get()).getHash())
                        .collect(Collectors.toList());
            });
            builder.add(this.block::getUncles);

        } else if (table == EthereumTable.TRANSACTION) {
            EthBlock.TransactionResult tr = txIter.next();
            EthBlock.TransactionObject tx = (EthBlock.TransactionObject) tr.get();

            builder.add(tx::getHash);
            builder.add(tx::getNonce);
            builder.add(tx::getBlockHash);
            builder.add(tx::getBlockNumber);
            builder.add(tx::getTransactionIndex);
            builder.add(tx::getFrom);
            builder.add(tx::getTo);
            builder.add(tx::getValue);
            builder.add(tx::getGas);
            builder.add(tx::getGasPrice);
            builder.add(tx::getInput);
        } else if (table == EthereumTable.ERC20) {
            while (logIter.hasNext()) {
                Log l = logIter.next();
                List<String> topics = l.getTopics();
                String data = l.getData();

                if (topics.get(0).equalsIgnoreCase(EthereumERC20Utils.TRANSFER_EVENT_TOPIC)) {
                    // Handle unindexed event fields:
                    // if the number of topics and fields in data part != 4, then it's a weird event
                    if (topics.size() < 3 && topics.size() + (data.length() - 2) / 64 != 4) {
                        continue;
                    }

                    if (topics.size() < 3) {
                        Iterator<String> dataFields = Splitter.fixedLength(64).split(data.substring(2)).iterator();
                        while (topics.size() < 3) {
                            topics.add("0x" + dataFields.next());
                    TransactionReceipt transactionReceipt = transactionReceiptOptional.get();
                    List<Log> logs = transactionReceipt.getLogs();
                    if (logs == null || logs.isEmpty()) {
                        continue;
                    }

                    for (Log l : logs) {
                        if (!l.getTopics().isEmpty() &&
                                l.getTopics().get(0).equalsIgnoreCase(EthereumERC20Utils.TRANSFER_EVENT_TOPIC) &&
                                l.getTopics().size() >= 3 &&
                                l.getData() != null && !l.getData().isEmpty() && !l.getData().equals("0x")) {

                            // Token contract address
                            builder.add(() -> String.format("%s", l.getAddress()));

                            // from address
                            builder.add(() -> h32ToH20(l.getTopics().get(1)));

                            // to address
                            builder.add(() -> h32ToH20(l.getTopics().get(2)));

                            // amount value
                            builder.add(() -> EthereumERC20Utils.hexToDouble(l.getData()));

                            builder.add(transactionReceipt::getTransactionHash);
                            builder.add(transactionReceipt::getBlockNumber);
                            this.suppliers = builder.build();
                            return true;
                        }
                        data = "0x" + dataFields.next();
                    }

                    // Token contract address
                    builder.add(() -> Optional.ofNullable(EthereumERC20Token.lookup.get(l.getAddress().toLowerCase()))
                            .map(Enum::name).orElse(String.format("ERC20(%s)", l.getAddress())));
                    // from address
                    builder.add(() -> h32ToH20(topics.get(1)));
                    // to address
                    builder.add(() -> h32ToH20(topics.get(2)));
                    // amount value
                    String finalData = data;
                    builder.add(() -> EthereumERC20Utils.hexToDouble(finalData));
                    builder.add(l::getTransactionHash);
                    builder.add(l::getBlockNumber);
                    this.suppliers = builder.build();
                    return true;
                }
            }

            return false;
        } else {
            return false;
        }

        this.suppliers = builder.build();
        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        return (boolean) suppliers.get(fieldToColumnIndex[field]).get();
    }

    @Override
    public long getLong(int field) {
        return ((Number) suppliers.get(fieldToColumnIndex[field]).get()).longValue();
    }

    @Override
    public double getDouble(int field) {
        return ((Number) suppliers.get(fieldToColumnIndex[field]).get()).doubleValue();
    }

    @Override
    public Slice getSlice(int field) {
        log.info("%d, %d, %s", field, fieldToColumnIndex[field], suppliers.get(fieldToColumnIndex[field]).get().toString());
        return Slices.utf8Slice((String) suppliers.get(fieldToColumnIndex[field]).get());
    }

    @Override
    public Object getObject(int field) {
        return serializeObject(columnHandles.get(field).getType(), null, suppliers.get(fieldToColumnIndex[field]).get());
    }

    @Override
    public boolean isNull(int field) {
        return suppliers.get(fieldToColumnIndex[field]).get() == null;
    }

    @Override
    public void close() {
    }

    private static long getLongExpressedValue(Object value) {
        if (value instanceof Date) {
            long storageTime = ((Date) value).getTime();
            // convert date from VM current time zone to UTC
            long utcMillis = storageTime + DateTimeZone.getDefault().getOffset(storageTime);
            return TimeUnit.MILLISECONDS.toDays(utcMillis);
        }
        if (value instanceof Timestamp) {
            long parsedJvmMillis = ((Timestamp) value).getTime();
            DateTimeZone jvmTimeZone = DateTimeZone.getDefault();
            long convertedMillis = jvmTimeZone.convertUTCToLocal(parsedJvmMillis);

            return convertedMillis;
        }
        if (value instanceof Float) {
            return floatToRawIntBits(((Float) value));
        }
        return ((Number) value).longValue();
    }

    private static Slice getSliceExpressedValue(Object value, Type type) {
        Slice sliceValue;
        if (value instanceof String) {
            sliceValue = Slices.utf8Slice((String) value);
        } else if (value instanceof byte[]) {
            sliceValue = Slices.wrappedBuffer((byte[]) value);
        } else if (value instanceof Integer) {
            sliceValue = Slices.utf8Slice(value.toString());
        } else {
            throw new IllegalStateException("unsupported string field type: " + value.getClass().getName());
        }
        if (isVarcharType(type)) {
            sliceValue = truncateToLength(sliceValue, type);
        }
        if (isCharType(type)) {
            sliceValue = truncateToLengthAndTrimSpaces(sliceValue, type);
        }

        return sliceValue;
    }

    private static Block serializeObject(Type type, BlockBuilder builder, Object object) {
        if (!isStructuralType(type)) {
            serializePrimitive(type, builder, object);
            return null;
        } else if (isArrayType(type)) {
            return serializeList(type, builder, object);
        } else if (isMapType(type)) {
            return serializeMap(type, builder, object);
        } else if (isRowType(type)) {
            return serializeStruct(type, builder, object);
        }
        throw new RuntimeException("Unknown object type: " + type);
    }

    private static Block serializeList(Type type, BlockBuilder builder, Object object) {
        List<?> list = (List) object;
        if (list == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 1, "list must have exactly 1 type parameter");
        Type elementType = typeParameters.get(0);

        BlockBuilder currentBuilder;
        if (builder != null) {
            currentBuilder = builder.beginBlockEntry();
        } else {
            currentBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), list.size());
        }

        for (Object element : list) {
            serializeObject(elementType, currentBuilder, element);
        }

        if (builder != null) {
            builder.closeEntry();
            return null;
        } else {
            Block resultBlock = currentBuilder.build();
            return resultBlock;
        }
    }

    private static Block serializeMap(Type type, BlockBuilder builder, Object object) {
        Map<?, ?> map = (Map) object;
        if (map == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        checkArgument(typeParameters.size() == 2, "map must have exactly 2 type parameter");
        Type keyType = typeParameters.get(0);
        Type valueType = typeParameters.get(1);
        boolean builderSynthesized = false;

        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        }
        BlockBuilder currentBuilder = builder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // Hive skips map entries with null keys
            if (entry.getKey() != null) {
                serializeObject(keyType, currentBuilder, entry.getKey());
                serializeObject(valueType, currentBuilder, entry.getValue());
            }
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        } else {
            return null;
        }
    }

    private static Block serializeStruct(Type type, BlockBuilder builder, Object object) {
        if (object == null) {
            requireNonNull(builder, "parent builder is null").appendNull();
            return null;
        }

        List<Type> typeParameters = type.getTypeParameters();
        EthBlock.TransactionObject structData = (EthBlock.TransactionObject) object;
        boolean builderSynthesized = false;
        if (builder == null) {
            builderSynthesized = true;
            builder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        }
        BlockBuilder currentBuilder = builder.beginBlockEntry();

        ImmutableList.Builder<Supplier> lstBuilder = ImmutableList.builder();
        lstBuilder.add(structData::getHash);
        lstBuilder.add(structData::getNonce);
        lstBuilder.add(structData::getBlockHash);
        lstBuilder.add(structData::getBlockNumber);
        lstBuilder.add(structData::getTransactionIndex);
        lstBuilder.add(structData::getFrom);
        lstBuilder.add(structData::getTo);
        lstBuilder.add(structData::getValue);
        lstBuilder.add(structData::getGas);
        lstBuilder.add(structData::getGasPrice);
        lstBuilder.add(structData::getInput);
        ImmutableList<Supplier> txColumns = lstBuilder.build();

        for (int i = 0; i < typeParameters.size(); i++) {
            serializeObject(typeParameters.get(i), currentBuilder, txColumns.get(i).get());
        }

        builder.closeEntry();
        if (builderSynthesized) {
            return (Block) type.getObject(builder, 0);
        } else {
            return null;
        }
    }

    private static void serializePrimitive(Type type, BlockBuilder builder, Object object) {
        requireNonNull(builder, "parent builder is null");

        if (object == null) {
            builder.appendNull();
            return;
        }

        if (BOOLEAN.equals(type)) {
            BOOLEAN.writeBoolean(builder, (Boolean) object);
        } else if (BIGINT.equals(type) || INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)
                || REAL.equals(type) || DATE.equals(type) || TIMESTAMP.equals(type)) {
            type.writeLong(builder, getLongExpressedValue(object));
        } else if (DOUBLE.equals(type)) {
            DOUBLE.writeDouble(builder, ((Number) object).doubleValue());
        } else if (isVarcharType(type) || VARBINARY.equals(type) || isCharType(type)) {
            type.writeSlice(builder, getSliceExpressedValue(object, type));
        } else {
            throw new UnsupportedOperationException("Unsupported primitive type: " + type);
        }
    }

    public static boolean isArrayType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    public static boolean isMapType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    public static boolean isRowType(Type type) {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    public static boolean isStructuralType(Type type) {
        String baseName = type.getTypeSignature().getBase();
        return baseName.equals(StandardTypes.MAP) || baseName.equals(StandardTypes.ARRAY) || baseName.equals(StandardTypes.ROW);
    }

    private static String h32ToH20(String h32) {
        return "0x" + h32.substring(EthereumMetadata.H32_BYTE_HASH_STRING_LENGTH - EthereumMetadata.H20_BYTE_HASH_STRING_LENGTH + 2);
    }
}

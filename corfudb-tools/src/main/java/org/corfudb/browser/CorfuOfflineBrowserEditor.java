package org.corfudb.browser;

import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.LogFormat;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.serializer.DynamicProtobufSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.corfudb.infrastructure.log.StreamLogFiles.parseHeader;

@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    private final DynamicProtobufSerializer dynamicProtobufSerializer;
    private final String QUOTE = "\"";

    public CorfuOfflineBrowserEditor(String offlineDbDir) {
        logDir = Paths.get(offlineDbDir, "log");
        System.out.println("Analyzing database located at :"+logDir);

        // prints header information for each of the corfu log files
        printHeader();
        /***
         * write methods that populate the cachedRegistryTable, cacheProtobufDescriptorTable,
         * the fdProtoMap and the messageFdProtoMap to replace the nulls below for the
         * new DynamicProtobufSerializer constructor
         */

        // System.out.println(listTables("CorfuSystem"));
        dynamicProtobufSerializer = new DynamicProtobufSerializer(null, null, null, null);

        // testing printAllProtoDescriptors
        System.out.println(printAllProtoDescriptors());
    }

    /**
     * Opens all log files one by one, and prints the header information for each Corfu log file.
     */
    public void printHeader() {
        System.out.println("Printing header information:");

        String[] extension = {"log"};
        File dir = logDir.toFile();

        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        for (File file : files) {
            LogFormat.LogHeader header;

            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                //StreamLogFiles a = StreamLogFiles(null, true);
                //header = StreamLogFiles.parseHeader(a, fileChannel, file.getAbsolutePath())
                header = parseHeader(null, fileChannel, file.getAbsolutePath());

                System.out.println(header);

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }

        }
    }

    @Override
    public EnumMap<IMetadata.LogUnitMetadataType, Object> printMetadataMap(long address) {
        return null;
    }

    @Override
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(String namespace, String tableName) {
        return null;
    }

    @Override
    public int printTable(String namespace, String tablename) {
        return 0;
    }

    @Override
    public int listTables(String namespace) {
        return 100;
    }

    @Override
    public int printTableInfo(String namespace, String tablename) {
        return 0;
    }

    @Override
    public int printAllProtoDescriptors() {
        return 200;
    }

    @Override
    public int clearTable(String namespace, String tablename) {
        return 0;
    }

    @Override
    public CorfuDynamicRecord addRecord(String namespace, String tableName, String newKey, String newValue, String newMetadata) {
        return null;
    }

    @Override
    public CorfuDynamicRecord editRecord(String namespace, String tableName, String keyToEdit, String newRecord) {
        return null;
    }

    @Override
    public int deleteRecordsFromFile(String namespace, String tableName, String pathToKeysFile, int batchSize) {
        return 0;
    }

    @Override
    public int deleteRecords(String namespace, String tableName, List<String> keysToDelete, int batchSize) {
        return 0;
    }

    @Override
    public int loadTable(String namespace, String tableName, int numItems, int batchSize, int itemSize) {
        return 0;
    }

    @Override
    public int listenOnTable(String namespace, String tableName, int stopAfter) {
        return 0;
    }

    @Override
    public Set<String> listStreamTags() {
        return null;
    }

    @Override
    public Map<String, List<CorfuStoreMetadata.TableName>> listTagToTableMap() {
        return null;
    }

    @Override
    public Set<String> listTagsForTable(String namespace, String table) {
        return null;
    }

    @Override
    public List<CorfuStoreMetadata.TableName> listTablesForTag(@Nonnull String streamTag) {
        return null;
    }
}

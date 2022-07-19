package io.lake.easylake;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


@SpringBootTest
class EasylakeApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	public void usingHadoopCatalog() {

		Configuration conf = new Configuration();
		conf.set("fs.jfs.impl","io.juicefs.JuiceFileSystem");
		conf.set("fs.defaultFS","jfs://myjfs/");
		conf.set("fs.AbstractFileSystem.jfs.impl","io.juicefs.JuiceFS");
		conf.set("juicefs.meta","mysql://root:eWJmP7yvpccHCtmVb61Gxl2XLzIrRgmT@(localhost:3306)/juicefs2_meta");
		String warehousePath = "/iceberg_warehouse";
		// String warehousePath = "file:///Users/huzekang/Downloads/easylake/iceberg_warehouse";
		HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

		Schema schema = new Schema(
				Types.NestedField.required(1, "level", Types.StringType.get()),
				Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
				Types.NestedField.required(3, "message", Types.StringType.get()),
				Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
		);

		PartitionSpec spec = PartitionSpec.builderFor(schema)
				.hour("event_time")
				.identity("level")
				.build();

		TableIdentifier name = TableIdentifier.of("logging.db", "logs");
		// 删除分区表
		catalog.dropTable(name);
		// 创建分区表
		Table table = catalog.createTable(name, schema, spec);

		// 加载表
		final Table loadTable = catalog.loadTable(name);
		System.out.println(loadTable.history().size());
	}

	@Test
	public void usingHadoopTable() {

		Schema schema = new Schema(
				Types.NestedField.required(1, "level", Types.StringType.get()),
				Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
				Types.NestedField.required(3, "message", Types.StringType.get())
				// Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
		);

		PartitionSpec spec = PartitionSpec.builderFor(schema)
				.hour("event_time")
				.identity("level")
				.build();
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/part_tb1";
		// 删除分区表
		tables.dropTable(tableLocation);
		// 创建分区表
		Table table = tables.create(schema, spec, tableLocation);

	}

	@Test
	public void createUnPartitionTable() {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.StringType.get()),
				Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
				Types.NestedField.required(3, "message", Types.StringType.get())
		);

		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb2";
		Table table = tables.create(schema, null, tableLocation);

	}


	@Test
	public void write2UnPartitionTable() throws IOException {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.IntegerType.get()),
				Types.NestedField.required(2, "event_time", Types.LongType.get()),
				Types.NestedField.required(3, "message", Types.StringType.get())
		);
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb3";
		tables.dropTable(tableLocation);
		Table table = tables.create(schema, null, tableLocation);

		// 写数据
		final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
		FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
				table.schema(), null);

		OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

		// 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
		final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
				fileFormat,
				appenderFactory,
				fileFactory,
				table.io(), 128 * 1024 * 1024);

		final GenericRecord gRecord = GenericRecord.create(schema);

		List<Record> expected = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			final Record record = gRecord
					.copy("id", i + 1, "event_time", System.currentTimeMillis(), "message", String.format("val-%d", i));
			expected.add(record);

			taskWriter.write(record);
		}
		WriteResult result = taskWriter.complete();
		System.out.println("新增文件数：" + result.dataFiles().length);
		System.out.println("删除文件数：" + result.deleteFiles().length);
		// 提交事务
		RowDelta rowDelta = table.newRowDelta();
		Arrays.stream(result.dataFiles()).forEach(dataFile -> rowDelta.addRows(dataFile));
		Arrays.stream(result.deleteFiles()).forEach(dataFile -> rowDelta.addDeletes(dataFile));

		rowDelta.validateDeletedFiles()
				.validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
				.commit();

	}

	@Test
	public void write2UnPartitionTableAppend() throws IOException {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.IntegerType.get()),
				Types.NestedField.required(2, "event_time", Types.LongType.get()),
				Types.NestedField.required(3, "message", Types.StringType.get())
		);
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb3";

		Table table = tables.load(tableLocation);

		// 写数据
		final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
		FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
				table.schema(), null);

		OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

		// 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
		final UnpartitionedWriter unpartitionedWriter = new UnpartitionedWriter(null, fileFormat, appenderFactory,
				fileFactory, table.io(), 128 * 1024 * 1024);


		final GenericRecord gRecord = GenericRecord.create(schema);

		List<Record> expected = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			final Record record = gRecord
					.copy("id", i + 10,
							"event_time", System.currentTimeMillis(),
							"message", String.format("新值-%d", i));
			expected.add(record);

			unpartitionedWriter.write(record);
		}
		WriteResult result = unpartitionedWriter.complete();
		System.out.println("新增文件数：" + result.dataFiles().length);
		System.out.println("删除文件数：" + result.deleteFiles().length);
		// 提交事务
		final AppendFiles appendFiles = table.newAppend();
		Arrays.stream(result.dataFiles()).forEach(dataFile -> appendFiles.appendFile(dataFile));

		appendFiles.commit();
	}

	@Test
	public void write2PartitionTable() throws IOException {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.IntegerType.get()),
				Types.NestedField.required(2, "province", Types.StringType.get()),
				Types.NestedField.required(3, "city", Types.StringType.get()),
				Types.NestedField.required(4, "message", Types.StringType.get())
		);
		PartitionSpec spec = PartitionSpec.builderFor(schema)
				.identity("province")
				.identity("city")
				.build();

		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/part_tb3";
		final ImmutableMap<String, String> pros = ImmutableMap.of(
				TableProperties.FORMAT_VERSION, "2"
		);
		tables.dropTable(tableLocation);
		Table table = tables.create(schema, spec, pros, tableLocation);

		// 写数据
		final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
		FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
				table.schema(), null);

		OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

		// 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
		final PartitionedWriter partitionedWriter = new PartitionedWriter(spec, fileFormat, appenderFactory,
				fileFactory, table.io(), 128 * 1024 * 1024) {
			@Override
			protected PartitionKey partition(Object row) {
				final GenericRecord genericRecord = (GenericRecord) row;
				final PartitionKey partitionKey = new PartitionKey(spec, schema);
				partitionKey.partition(genericRecord);
				return partitionKey;
			}
		};

		final GenericRecord gRecord = GenericRecord.create(schema);

		List<Record> expected = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			final HashMap<String, Object> hashMap = Maps.newHashMap();
			hashMap.put("id", i + 1);
			hashMap.put("province", "gd");
			hashMap.put("city", "shenzhen");
			hashMap.put("message", String.format("msg-%d", i));
			final Record record = gRecord.copy(hashMap);
			expected.add(record);

			partitionedWriter.write(record);
		}
		WriteResult result = partitionedWriter.complete();
		System.out.println("新增文件数：" + result.dataFiles().length);
		System.out.println("删除文件数：" + result.deleteFiles().length);
		// 提交事务
		RowDelta rowDelta = table.newRowDelta();
		Arrays.stream(result.dataFiles()).forEach(dataFile -> rowDelta.addRows(dataFile));
		Arrays.stream(result.deleteFiles()).forEach(dataFile -> rowDelta.addDeletes(dataFile));

		rowDelta.validateDeletedFiles()
				.validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
				.commit();

	}

	@Test
	public void replace2PartitionTable() throws IOException {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.IntegerType.get()),
				Types.NestedField.required(2, "province", Types.StringType.get()),
				Types.NestedField.required(3, "city", Types.StringType.get()),
				Types.NestedField.required(4, "message", Types.StringType.get())
		);
		PartitionSpec spec = PartitionSpec.builderFor(schema)
				.identity("province")
				.identity("city")
				.build();

		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/part_tb3";
		Table table = tables.load(tableLocation);

		// 写数据
		final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
		FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
				table.schema(), null);

		OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

		// 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
		final PartitionedWriter partitionedWriter = new PartitionedWriter(spec, fileFormat, appenderFactory,
				fileFactory, table.io(), 128 * 1024 * 1024) {
			@Override
			protected PartitionKey partition(Object row) {
				final GenericRecord genericRecord = (GenericRecord) row;
				final PartitionKey partitionKey = new PartitionKey(spec, schema);
				partitionKey.partition(genericRecord);
				return partitionKey;
			}
		};

		final GenericRecord gRecord = GenericRecord.create(schema);

		List<Record> expected = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			final HashMap<String, Object> hashMap = Maps.newHashMap();
			hashMap.put("id", i + 1);
			hashMap.put("province", "gd");
			hashMap.put("city", "shenzhen");
			hashMap.put("message", String.format("msg-%d", i * 20));
			final Record record = gRecord.copy(hashMap);
			expected.add(record);

			partitionedWriter.write(record);
		}
		WriteResult result = partitionedWriter.complete();
		System.out.println("新增文件数：" + result.dataFiles().length);
		System.out.println("删除文件数：" + result.deleteFiles().length);

		//ReplacePartitions
		final ReplacePartitions replacePartitions = table.newReplacePartitions();
		Arrays.stream(result.dataFiles()).forEach(e->replacePartitions.addFile(e));
		replacePartitions.commit();

	}


	@Test
	public void replace2UnPartitionTable() throws IOException {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.IntegerType.get()),
				Types.NestedField.required(2, "event_time", Types.LongType.get()),
				Types.NestedField.required(3, "message", Types.StringType.get())
		);
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb6";
		Table table = tables.load( tableLocation);

		// 写数据
		final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
		FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
				table.schema(), null);

		OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

		// 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
		final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
				fileFormat,
				appenderFactory,
				fileFactory,
				table.io(), 128 * 1024 * 1024);

		final GenericRecord gRecord = GenericRecord.create(schema);

		List<Record> expected = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			final Record record = gRecord
					.copy("id", i + 1, "event_time", System.currentTimeMillis(), "message", String.format("val-%d", i));
			expected.add(record);

			taskWriter.write(record);
		}
		WriteResult result = taskWriter.complete();
		System.out.println("新增文件数：" + result.dataFiles().length);
		System.out.println("删除文件数：" + result.deleteFiles().length);
		// 提交事务
		final ReplacePartitions replacePartitions = table.newReplacePartitions();
		Arrays.stream(result.dataFiles()).forEach(dataFile -> replacePartitions.addFile(dataFile));
		replacePartitions.commit();

	}

	@Test
	public void overwrite2UnPartitionTable() throws IOException {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.IntegerType.get()),
				Types.NestedField.required(2, "event_time", Types.LongType.get()),
				Types.NestedField.required(3, "message", Types.StringType.get())
		);
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb6";
		Table table = tables.load( tableLocation);

		// 写数据
		final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
		FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
				table.schema(), null);

		OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

		// 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
		final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
				fileFormat,
				appenderFactory,
				fileFactory,
				table.io(), 128 * 1024 * 1024);

		final GenericRecord gRecord = GenericRecord.create(schema);

		List<Record> expected = Lists.newArrayList();
		for (int i = 0; i < 5; i++) {
			final Record record = gRecord
					.copy("id", i + 1, "event_time", System.currentTimeMillis(), "message", String.format("val-%d", i));
			expected.add(record);

			taskWriter.write(record);
		}
		WriteResult result = taskWriter.complete();
		System.out.println("新增文件数：" + result.dataFiles().length);
		System.out.println("删除文件数：" + result.deleteFiles().length);
		// 提交事务
		final OverwriteFiles overwriteFiles = table.newOverwrite();
		overwriteFiles.overwriteByRowFilter(Expressions.alwaysTrue());
		Arrays.stream(result.dataFiles()).forEach(dataFile -> overwriteFiles.addFile(dataFile));
		overwriteFiles.commit();

	}

	@Test
	public void readTable() {
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb3";
		final Table table = tables.load(tableLocation);

		table.history().forEach(historyEntry -> {
			System.out.println("快照id:"+historyEntry.snapshotId());
		});

		table.snapshots().forEach(snapshot -> {
			System.out.println("--------------快照概要：" + snapshot.snapshotId());
			snapshot.summary().forEach((k, v) -> {
				System.out.println(k + ":" + v);
			});
		});


		// 根据数据找文件
		TableScan scan = table.newScan();
		TableScan filteredScan = scan.filter(Expressions.equal("id", 10));
		Schema projection = scan.schema();
		Iterable<CombinedScanTask> tasks = filteredScan.planTasks();

		for (CombinedScanTask task : tasks) {
			for (FileScanTask file : task.files()) {
				System.out.println("数据所在文件："+file.file().toString());
			}
		}

		// 根据字段找每一行数据
		CloseableIterable<Record> result = IcebergGenerics.read(table)
				.where(Expressions.greaterThan("id", 10))
				.build();
		for (Record record : result) {
			System.out.println(record.toString());
		}

	}

	@Test
	public void expireMetadata() {
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb5";
		final Table table = tables.load(tableLocation);
		// 删除过期的元数据快照: 会同时删除Manifests【xxx.avro】  和 Manifests Lists【snap-xxx.avro】
		long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 1);
		table.expireSnapshots()
				.expireOlderThan(tsToExpire)
				.commit();
	}

	@Test
	public void createFormatV2Table() {
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		Schema schema = new Schema(
				Types.NestedField.required(1, "level", Types.StringType.get()),
				Types.NestedField.required(2, "event_time", Types.LongType.get()),
				Types.NestedField.required(3, "message", Types.StringType.get())
		);

		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Volumes/Samsung_T5/opensource/easylake/data/iceberg_warehouse/iceberg_warehouse/tb5";
		final ImmutableMap<String, String> pros = ImmutableMap.of(
				TableProperties.FORMAT_VERSION, "2",
				TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
				TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1",
				TableProperties.MAX_SNAPSHOT_AGE_MS, 1000 * 60 * 2 + "",
				TableProperties.MANIFEST_MIN_MERGE_COUNT, "2"
		);
		tables.dropTable(tableLocation);
		tables.create(schema, null, null, pros, tableLocation);
	}

	private static class MyTaskWriter extends BaseTaskWriter<Record> {

		private RollingFileWriter currentWriter;

		private MyTaskWriter(PartitionSpec spec, FileFormat format,
				FileAppenderFactory<Record> appenderFactory,
				OutputFileFactory fileFactory, FileIO io,
				long targetFileSize) {
			super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
			this.currentWriter = new RollingFileWriter(null);

		}

		@Override
		public void write(Record row) throws IOException {
			currentWriter.write(row);
		}


		@Override
		public void close() throws IOException {
			if (currentWriter != null) {
				currentWriter.close();
			}

		}
	}


}

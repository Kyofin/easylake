package io.lake.easylake;

import com.google.common.collect.Lists;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.PartitionSpec;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


@SpringBootTest
class EasylakeApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	public void usingHadoopCatalog() {

		Configuration conf = new Configuration();
		String warehousePath = "file:///Users/huzekang/Downloads/easylake/iceberg_warehouse";
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
		final String tableLocation = "file:///Users/huzekang/Downloads/easylake/iceberg_warehouse/tb1";
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
		final String tableLocation = "file:///Users/huzekang/Downloads/easylake/iceberg_warehouse/tb2";
		Table table = tables.create(schema, null, tableLocation);

	}


	@Test
	public void write2Table() throws IOException {
		Schema schema = new Schema(
				Types.NestedField.required(1, "id", Types.IntegerType.get()),
				Types.NestedField.required(2, "event_time", Types.LongType.get()),
				Types.NestedField.required(3, "message", Types.StringType.get())
		);
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Users/huzekang/Downloads/easylake/iceberg_warehouse/tb3";
		tables.dropTable(tableLocation);
		Table table = tables.create(schema, null, tableLocation);

		// 写数据
		final FileFormat fileFormat = FileFormat.valueOf("AVRO");
		FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
				table.schema(), null);
		;
		OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
		;
		final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
				fileFormat,
				appenderFactory,
				fileFactory,
				table.io(), 128 * 1024 * 1024);

		final GenericRecord gRecord = GenericRecord.create(schema);

		List<Record> expected = Lists.newArrayList();
		for (int i = 0; i < 20; i++) {
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

		//
		// // todo用 TaskWriter生成 Datafile
		//
		// 初始化事务
		Transaction t = table.newTransaction();

		// commit operations to the transaction
		for (DataFile dataFile : result.dataFiles()) {
			t.newAppend()
					.appendFile(dataFile)
					.appendFile(dataFile)
					.appendFile(dataFile)
					.commit();
		}

		// commit all the changes to the table
		t.commitTransaction();
	}

	@Test
	public void readTable() {
		Configuration conf = new Configuration();
		HadoopTables tables = new HadoopTables(conf);
		// 不指定namespace和表名，直接指定路径
		final String tableLocation = "file:///Users/huzekang/Downloads/easylake/iceberg_warehouse/tb3";
		final Table table = tables.load(tableLocation);
		table.snapshots().forEach(snapshot -> {
			System.out.println("--------------" + snapshot.snapshotId());
			snapshot.summary().forEach((k, v) -> {
				System.out.println(k + ":" + v);
			});
		});

	}

	private static class MyTaskWriter extends BaseTaskWriter<Record> {

		private RollingFileWriter dataWriter;

		private MyTaskWriter(PartitionSpec spec, FileFormat format,
				FileAppenderFactory<Record> appenderFactory,
				OutputFileFactory fileFactory, FileIO io,
				long targetFileSize) {
			super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
			this.dataWriter = new RollingFileWriter(null);

		}

		@Override
		public void write(Record row) throws IOException {
			dataWriter.write(row);
		}


		@Override
		public void close() throws IOException {
			if (dataWriter != null) {
				dataWriter.close();
			}

		}
	}


}

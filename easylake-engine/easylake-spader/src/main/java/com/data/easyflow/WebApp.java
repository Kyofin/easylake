package com.data.easyflow;

import com.alibaba.fastjson.JSONObject;
import com.data.easyflow.grpc.HelloWorldServer;
import com.data.easyflow.service.CsvService;
import io.javalin.Javalin;
import io.javalin.core.event.EventHandler;
import org.apache.spark.sql.SparkSession;
import java.nio.charset.StandardCharsets;

/**
 * @program: easyflow
 * @author: huzekang
 * @create: 2022-07-07 16:21
 **/
public class WebApp {

    public static void main(String[] args) {

        Javalin app = Javalin.create().events(eventListener -> {
            final HelloWorldServer server = new HelloWorldServer();
            eventListener.serverStarted(new EventHandler() {
                @Override
                public void handleEvent() throws Exception {
                    SparderSparkSession.initSparkSession();
                    server.start();
                    // server.blockUntilShutdown();
                }
            });

            eventListener.serverStopping(new EventHandler() {
                @Override
                public void handleEvent() throws Exception {
                    SparderSparkSession.closeSparkSession();
                    server.stop();
                }
            });
        }).start(7070);

        app.post("/tryRun", ctx -> {
            final String jobContent = ctx.formParam("jobContent");
            final SparkSession spark = SparderSparkSession.getSparkSession();
            final Engine engine = new Engine(jobContent, true, spark);
            final String result = engine.exec();
            ctx.result(result);
        });

        app.post("/csvParse", ctx -> {
            final String filePath = ctx.formParam("filePath");
            final String charset = ctx.formParam("charset") == null ? StandardCharsets.UTF_8.name() : ctx.formParam("charset");
            final String sep = ctx.formParam("sep") == null ? "," : ctx.formParam("sep");
            final SparkSession spark = SparderSparkSession.getSparkSession();
            final JSONObject jsonObject = CsvService.parseSchemaAndPreview(spark, filePath, charset, sep);
            ctx.json(jsonObject);
        });

        app.get("/sleep", ctx -> {
            final SparkSession spark = SparderSparkSession.getSparkSession();
            spark.sparkContext().setLocalProperty("spark.scheduler.pool", "lightweight_tasks");
            spark.sql("SELECT java_method('java.lang.Thread', 'sleep', 15000l) from range(1,3,1,2)").show();
            spark.sparkContext().setLocalProperty("spark.scheduler.pool", null);

            ctx.result("ok!");
        });

        app.post("/exec", ctx -> {
            final String jobContent = ctx.formParam("jobContent");
            final SparkSession spark = SparderSparkSession.getSparkSession();
            final Engine engine = new Engine(jobContent, false, spark);
            final String result = engine.exec();
            ctx.result("finish!");
        });
    }
}

package WuzzufApplication.controller;

import WuzzufApplication.service.spark;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;

import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.awt.*;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.regexp_extract;

import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;


@RestController
public class Controller {

    @Value("${app.name}")
    String name;

    private Dataset<Row> csvDataFrame = null;
    private Dataset<Row> csvDataFrame_clean = null;
    private Dataset<Row> wuzzufSql = null;

    @Autowired
    private SparkSession sparkSession;

    public void readData() {
        if (csvDataFrame != null)
            return;
        csvDataFrame = sparkSession.read()
                .format("csv").option("header", "true")
                .option("delimiter", ",")
                .option("inferSchema", "true")
                .load("src/main/resources/Wuzzuf_Jobs.csv");
    }

    @GetMapping(value = "/sparkTask1")
    public String sparkTask1(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 1;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        readData();
        spark.Tasks[T] = n;
        spark.Tasks[T] = asTable(csvDataFrame.showString(Integer.parseInt(n), 1000000, false), "\\|");
        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask1")
    public String jsonSparkTask1(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 2;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];

        readData();
        csvDataFrame.createOrReplaceTempView("WUZZUF_DATA");
        final Dataset<Row> wuzzuf_sql = sparkSession
                .sql("SELECT CAST(Title as String) title, CAST(Company as String) company, CAST(Location as String) location, "
                        + "CAST(Type as String) type, CAST(Level as String) level, CAST(YearsExp as String) yearsExp, "
                        + "CAST(Country as String) country, CAST(Skills as String) skills FROM WUZZUF_DATA LIMIT " + n);
        spark.Tasks[T] = n;
        spark.Tasks[T] = wuzzuf_sql.toJSON().collectAsList().toString();
        return spark.Tasks[T];
    }

    @GetMapping(value = "/sparkTask2")
    public String sparkTask2() {
        int T = 3;
        if (spark.Tasks[T] != "")
            return spark.Tasks[T];
        readData();
        spark.Tasks[T] = asTable(csvDataFrame.summary().showString(20, 1000000, false), "\\|");
        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask2")
    public String jsonSparkTask2() {
        int T = 4;
        if (spark.Tasks[T] != "")
            return spark.Tasks[T];
        readData();
        spark.Tasks[T] = csvDataFrame.summary().toJSON().collectAsList().toString();
        return spark.Tasks[T];
    }

    public void cleanData() {
        if (wuzzufSql != null)
            return;
        readData();
        csvDataFrame_clean = csvDataFrame.dropDuplicates().na().drop("any")
                .filter((FilterFunction<Row>) row -> !row.get(5).equals("null Yrs of Exp"));

        csvDataFrame_clean.createOrReplaceTempView("WUZZUF_DATA");
        wuzzufSql = sparkSession
                .sql("SELECT CAST(Title as String) title"
                        + ", CAST(Company as String) company"
                        + ", CAST(Location as String) location"
                        + ", CAST(Type as String) type"
                        + ", CAST(Level as String) level"
                        + ", CAST(YearsExp as String) yearsExp"
                        + ", CAST(Country as String) country"
                        + ", CAST(Skills as String) skills"
                        + " FROM WUZZUF_DATA");
    }

    public String PieChart(Dataset<Row> data, String imageName) {
        PieChart chart = new PieChartBuilder().width(800).height(600).title(getClass().getSimpleName()).build();
        Color[] sliceColors = new Color[]{new Color(224, 68, 14), new Color(230, 105, 62), new Color(236, 143, 110), new Color(243, 180, 159), new Color(246, 199, 182)};
        chart.getStyler().setSeriesColors(sliceColors);
        for (int i = 0; i < data.collectAsList().size(); i++) {
            chart.addSeries(String.valueOf(data.collectAsList().get(i).get(0)), (Long) (data.collectAsList().get(i).get(1)));
        }
        try {
            BitmapEncoder.saveJPGWithQuality(chart, "public/" + imageName, 0.95f);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "<img src= '" + imageName + "'>";
    }

    @GetMapping(value = "/sparkTask4")
    public String sparkTask4(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 5;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();
        wuzzufSql.createOrReplaceTempView("COUNT_JOBS_COMPANY");
        Dataset<Row> COUNT_JOBS_COMPANY = sparkSession.sql("SELECT company, count(title) job_count "
                + "FROM COUNT_JOBS_COMPANY "
                + "GROUP BY company "
                + "ORDER BY job_count desc limit " + n
        );
        String sql = COUNT_JOBS_COMPANY.showString(Integer.parseInt(n), 1000000, false);
        String image = PieChart(COUNT_JOBS_COMPANY, "COUNT_JOBS_COMPANY.jpg");
        spark.N[T] = n;
        spark.Tasks[T] = "<div>" + asTable(sql, "\\|") + image + "</div>";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask4")
    public String jsonSparkTask4(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 6;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();
        wuzzufSql.createOrReplaceTempView("COUNT_JOBS_COMPANY");
        spark.N[T] = n;
        Dataset<Row> COUNT_JOBS_COMPANY = sparkSession.sql("SELECT company, count(title) job_count "
                + "FROM COUNT_JOBS_COMPANY "
                + "GROUP BY company "
                + "ORDER BY job_count desc limit " + n
        );

        String image = PieChart(COUNT_JOBS_COMPANY, "COUNT_JOBS_COMPANY.jpg");
        spark.Tasks[T] = COUNT_JOBS_COMPANY.toJSON().collectAsList().toString();
        spark.Tasks[T] = spark.Tasks[T].substring(0, spark.Tasks[T].length() - 1) + ", {\"url\":\"" + image.substring(11,image.length()-2) + "\"}]";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/sparkTask6")
    public String sparkTask6(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 7;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();
        wuzzufSql.createOrReplaceTempView("COUNT_JOBS");
        Dataset<Row> COUNT_JOBS = sparkSession.sql("SELECT title, count(title) job_count "
                + "FROM COUNT_JOBS "
                + "GROUP BY title "
                + "ORDER BY job_count desc limit " + n
        );
        String sql = COUNT_JOBS.showString(Integer.parseInt(n), 1000000, false);
        String image = PieChart(COUNT_JOBS, "COUNT_JOBS.jpg");
        spark.N[T] = n;
        spark.Tasks[T] = "<div>" + asTable(sql, "\\|") + image + "</div>";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask6")
    public String jsonSparkTask6(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 8;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();
        wuzzufSql.createOrReplaceTempView("COUNT_JOBS");
        spark.N[T] = n;
        Dataset<Row> COUNT_JOBS = sparkSession.sql("SELECT title, count(title) job_count "
                + "FROM COUNT_JOBS "
                + "GROUP BY title "
                + "ORDER BY job_count desc limit " + n
        );

        String image = PieChart(COUNT_JOBS, "COUNT_JOBS.jpg");
        spark.Tasks[T] = COUNT_JOBS.toJSON().collectAsList().toString();
        spark.Tasks[T] = spark.Tasks[T].substring(0, spark.Tasks[T].length() - 1) + ", {\"url\":\"" + image.substring(11,image.length()-2) + "\"}]";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/sparkTask8")
    public String sparkTask8(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 9;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        spark.N[T] = n;
        cleanData();
        wuzzufSql.createOrReplaceTempView("COUNT_LOCATIONS");
        Dataset<Row> COUNT_LOCATIONS = sparkSession.sql("SELECT location, count(location) location_count "
                + "FROM COUNT_LOCATIONS "
                + "GROUP BY location "
                + "ORDER BY location_count desc limit " + n
        );

        String sql = COUNT_LOCATIONS.showString(Integer.parseInt(n), 1000000, false);
        String image = PieChart(COUNT_LOCATIONS, "COUNT_LOCATIONS.jpg");
        spark.Tasks[T] = "<div>" + asTable(sql, "\\|") + image + "</div>";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask8")
    public String jsonSparkTask8(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 10;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();
        wuzzufSql.createOrReplaceTempView("COUNT_LOCATIONS");
        Dataset<Row> COUNT_LOCATIONS = sparkSession.sql("SELECT location, count(location) location_count "
                + "FROM COUNT_LOCATIONS "
                + "GROUP BY location "
                + "ORDER BY location_count desc limit " + n
        );

        String image = PieChart(COUNT_LOCATIONS, "COUNT_LOCATIONS.jpg");
        spark.Tasks[T] = COUNT_LOCATIONS.toJSON().collectAsList().toString();
        spark.Tasks[T] = spark.Tasks[T].substring(0, spark.Tasks[T].length() - 1) + ", {\"url\":\"" + image.substring(11,image.length()-2) + "\"}]";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/sparkTask10")
    public String sparkTask10(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 11;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();

        wuzzufSql.createOrReplaceTempView("SKILLS");
        Dataset<Row> skillsDF = sparkSession.sql("SELECT skills FROM SKILLS");

        Dataset<String> skillListDF = skillsDF.flatMap((Row r) -> {
            List<String> ls = Arrays.stream(r.toString().split(",")).collect(Collectors.toList());
            return ls.iterator();
        }, Encoders.STRING());

        List<String> skillsList = skillListDF.collectAsList();
        for (int i = 0; i < skillsList.size(); ++i) {
            String skill = skillsList.get(i);
            int f = 0, l = skill.length();
            if (skill.startsWith("[") || skill.startsWith(" "))
                f = 1;
            if (skill.endsWith("]"))
                l -= 1;
            skillsList.set(i, skill.substring(f, l));
            System.out.println(skillsList.get(i));
        }

        skillsDF = new SQLContext(sparkSession)
                .createDataset(skillsList, Encoders.STRING()).toDF("skill");

        skillsDF.createOrReplaceTempView("skillsListSQL");
        Dataset<Row> skillsListSQL = sparkSession.sql("SELECT skill, COUNT(skill) skillCount"
                + " FROM skillsListSQL"
                + " GROUP BY skill"
                + " ORDER BY skillCount DESC limit " + n);
        String image = PieChart(skillsListSQL, "skillsListSQL.jpg");
        String sql = skillsListSQL.showString(Integer.parseInt(n), 1000000, false);
        spark.N[T] = n;
        spark.Tasks[T] = "<div>" + asTable(sql, "\\|") + image + "</div>";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask10")
    public String jsonSparkTask10(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 12;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();

        wuzzufSql.createOrReplaceTempView("SKILLS");
        Dataset<Row> skillsDF = sparkSession.sql("SELECT skills FROM SKILLS");

        Dataset<String> skillListDF = skillsDF.flatMap((Row r) -> {
            List<String> ls = Arrays.stream(r.toString().split(",")).collect(Collectors.toList());
            return ls.iterator();
        }, Encoders.STRING());

        List<String> skillsList = skillListDF.collectAsList();
        for (int i = 0; i < skillsList.size(); ++i) {
            String skill = skillsList.get(i);
            int f = 0, l = skill.length();
            if (skill.startsWith("[") || skill.startsWith(" "))
                f = 1;
            if (skill.endsWith("]"))
                l -= 1;
            skillsList.set(i, skill.substring(f, l));
            System.out.println(skillsList.get(i));
        }

        skillsDF = new SQLContext(sparkSession)
                .createDataset(skillsList, Encoders.STRING()).toDF("skill");

        skillsDF.createOrReplaceTempView("skillsListSQL");
        spark.N[T] = n;
        Dataset<Row> skillsListSQL = sparkSession.sql("SELECT skill, COUNT(skill) skillCount"
                + " FROM skillsListSQL"
                + " GROUP BY skill"
                + " ORDER BY skillCount DESC limit " + n);

        String image = PieChart(skillsListSQL, "skillsListSQL.jpg");
        spark.Tasks[T] = skillsListSQL.toJSON().collectAsList().toString();
        spark.Tasks[T] = spark.Tasks[T].substring(0, spark.Tasks[T].length() - 1) + ", {\"url\":\"" + image.substring(11,image.length()-2) + "\"}]";
        return spark.Tasks[T];
    }

    @GetMapping(value = "/sparkTask11")
    public String sparkTask11(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 13;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();
        wuzzufSql.createOrReplaceTempView("YearsExpView");
        Dataset<Row> yearsExpDF = sparkSession.sql("SELECT yearsExp FROM YearsExpView");
        yearsExpDF = yearsExpDF.withColumn("yearsExpInt", regexp_extract(yearsExpDF.col("yearsExp"), "^\\d+", 0));
        String sql = yearsExpDF.showString(Integer.parseInt(n), 100, false);
        spark.N[T] = n;
        spark.Tasks[T] = asTable(sql, "\\|");
        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask11")
    public String jsonSparkTask11(@RequestParam(value = "n", defaultValue = "10") String n) {
        int T = 14;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == n)
                return spark.Tasks[T];
        cleanData();
        wuzzufSql.createOrReplaceTempView("YearsExpView");
        Dataset<Row> yearsExpDF = sparkSession.sql("SELECT yearsExp FROM YearsExpView");
        yearsExpDF = yearsExpDF.withColumn("yearsExpInt", regexp_extract(yearsExpDF.col("yearsExp"), "^\\d+", 0));

        spark.Tasks[T] = yearsExpDF.toJSON().collectAsList().toString();
        spark.N[T] = n;
        return spark.Tasks[T];
    }

    @GetMapping(value = "/sparkTask12")
    public String sparkTask12(@RequestParam(value = "k", defaultValue = "2") String k) {
        int T = 15;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == k)
                return spark.Tasks[T];

        cleanData();

        wuzzufSql.createOrReplaceTempView("KMeans");
        Dataset<Row> KMeansDF = sparkSession.sql("SELECT title, company FROM KMeans");

        StringIndexer si = new StringIndexer();
        si.setInputCols(KMeansDF.columns());
        String[] indexCols = {"titleIndex", "companyIndex"};
        si.setOutputCols(indexCols);
        KMeansDF = si.fit(KMeansDF).transform(KMeansDF);

        VectorAssembler va = new VectorAssembler();
        va.setInputCols(indexCols);
        va.setOutputCol("features");
        KMeansDF = va.transform(KMeansDF);

        KMeansModel model = new KMeans().setK(Integer.parseInt(k)).setSeed(1L).fit(KMeansDF);

        Dataset<Row> predictions = model.transform(KMeansDF);

        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        double silhouette = evaluator.evaluate(predictions);
        spark.Tasks[T] = String.format("\n%f\n", silhouette);

        spark.Tasks[T] += asTable(predictions.showString(100, 1000, false), "\\|");

        return spark.Tasks[T];
    }

    @GetMapping(value = "/json/sparkTask12")
    public String jsonSparkTask12(@RequestParam(value = "k", defaultValue = "2") String k) {
        int T = 16;
        if (spark.Tasks[T] != "")
            if (spark.N[T] != "" && spark.N[T] == k)
                return spark.Tasks[T];

        cleanData();

        wuzzufSql.createOrReplaceTempView("KMeans");
        Dataset<Row> KMeansDF = sparkSession.sql("SELECT title, company FROM KMeans");

        StringIndexer si = new StringIndexer();
        si.setInputCols(KMeansDF.columns());
        String[] indexCols = {"titleIndex", "companyIndex"};
        si.setOutputCols(indexCols);
        KMeansDF = si.fit(KMeansDF).transform(KMeansDF);

        VectorAssembler va = new VectorAssembler();
        va.setInputCols(indexCols);
        va.setOutputCol("features");
        KMeansDF = va.transform(KMeansDF);

        KMeansModel model = new KMeans().setK(2).setSeed(1L).fit(KMeansDF);

        Dataset<Row> predictions = model.transform(KMeansDF);

        ClusteringEvaluator evaluator = new ClusteringEvaluator();

        double silhouette = evaluator.evaluate(predictions);
        spark.Tasks[T] = "[ {\"accuracy\":" + String.format("\n%f\n", silhouette)+ " }, ";

        spark.Tasks[T] += predictions.select("title", "company", "prediction")
                .toJSON().collectAsList().toString().substring(1);

        return spark.Tasks[T];
    }

    public String asTable(String table, String colDelim) {
        ArrayList<String> splits = new ArrayList<>(Arrays.asList(table.split("\\s*\n\\s*")));

        StringBuilder t = new StringBuilder();

        t.append("<style>\n");
        t.append("img{position:fixed;top:0;right:0;width:800px;height:600px}\n");
        t.append("td, th {padding:0 10 0 10;}\n");
        t.append("#hrHeader {width:100%;}\n");
        t.append("#hrInTable {border:none;\n" +
                "  border-top:1px dashed #f00;\n" +
                "  color:#fff;\n" +
                "  background-color:#fff;\n" +
                "  height:1px;\n" +
                "  width:90%;\n" +
                "}\n");
        t.append("table{ display:block; table-layout:fixed; cell-max-width:100%;}>\n");
        t.append("</style>\n");

        t.append("<table>\n");
        try {
            String[] __ = splits.get(1).split("\\s*" + colDelim + "\\s*");

            __ = ArrayUtils.remove(__, 0);

            t.append("<tr><td colspan=\"" + __.length + "\"> <hr id=\"hrHeader\"> </td></tr>\n");

            StringBuilder t_ = new StringBuilder();

            for (String token : __)
                t_.append("<th>" + token + "</th>");

            t.append("<tr>" + t_.toString() + "</tr>\n");

            t.append("<tr><td colspan=\"" + __.length + "\"> <hr id=\"hrHeader\"> </td></tr>\n");

            splits.remove(0);
            splits.remove(0);
            splits.remove(0);
        } catch (Exception e) {
            System.out.println(e);
            t.append("\nFailed to Convert Dataset showString to HTML Table\n");
            t.append(table);
            return t.toString();
        }

        for (String line : splits) {
            String[] __ = line.split("\\s*" + colDelim + "\\s*");
            __ = ArrayUtils.remove(__, 0);
            if (__.length == 0) break;
            StringBuilder t_ = new StringBuilder();
            for (String token : __)
                t_.append("<td>" + token + "</td>");
            t.append("<tr>" + t_.toString() + "</tr>\n");
            t.append("<tr><td colspan=\"" + __.length + "\"> <hr id=\"hrInTable\"> </td></tr>\n");
        }

        t.append("</table>\n");
        return t.toString();
    }

}

package org.ai.tryouts;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.records.reader.impl.jackson.FieldSelection;
import org.datavec.api.records.reader.impl.jackson.FieldSelection.Builder;
import org.datavec.api.records.reader.impl.jackson.JacksonRecordReader;
import org.datavec.api.split.InputSplit;
import org.datavec.api.split.NumberedFileInputSplit;
import org.datavec.api.transform.analysis.DataAnalysis;
import org.datavec.api.transform.schema.Schema;
import org.datavec.api.transform.ui.HtmlAnalysis;
import org.datavec.api.util.ClassPathResource;
import org.datavec.api.writable.Writable;
import org.datavec.local.transforms.AnalyzeLocal;
import org.datavec.spark.transform.AnalyzeSpark;
import org.nd4j.shade.jackson.core.JsonFactory;
import org.nd4j.shade.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Conduct and export some basic analysis on the Iris dataset, as a stand-alone .html file.
 * <p>
 * This functionality is still fairly basic, but can still be useful for analysis and debugging.
 *
 * @author Alex Black
 */
public class ClimbingAnalysis {

    public static void main(String[] args) throws Exception {
        analyze();
        //createFiles();
    }

    private static void analyze() throws Exception {
        String path = ClimbingAnalysis.class.getClassLoader()
                .getResource("climbData/routeLoc_012.txt").getFile()
                .replace("12", "%d");

        InputSplit is = new NumberedFileInputSplit(path, 0, 48);

        FieldSelection selection = (new Builder())
                .addField("name")
                .addField("id")
                .addField("rating")
                .addField("type")
                .addField("stars")
                .addField("starVotes")
                .addField("loc_0")
                .addField("loc_1")
                .addField("loc_2")
                .addField("loc_3")
                .addField("loc_4")
                .addField("loc_5")
                .addField("latitude")
                .addField("longitude")
                .build();

        RecordReader rr = new JacksonRecordReader(selection, new ObjectMapper(new JsonFactory()));
        rr.initialize(is);


        Schema schema = new Schema.Builder()
                .addColumnString("name")
                .addColumnInteger("id")
                .addColumnCategorical("rating", "5.10b", "5.12b", "5.10a")
                .addColumnCategorical("type", "Sport", "Trad", "TR", "Aid")
                .addColumnInteger("stars")
                .addColumnInteger("starVotes")
                .addColumnCategorical("loc_0")
                .addColumnCategorical("loc_1")
                .addColumnCategorical("loc_2")
                .addColumnCategorical("loc_3")
                .addColumnCategorical("loc_4")
                .addColumnCategorical("loc_5")
                .addColumnsDouble("latitude")
                .addColumnsDouble("longitude")
                .build();

        DataAnalysis da = AnalyzeLocal.analyze(schema, rr);
        AnalyzeSpark ap;

        //rr.reset();


        //DataQualityAnalysis daQuality = AnalyzeLocal.analyzeQuality(schema, rr);

        System.out.println(da);
        //System.out.println(daQuality);
        HtmlAnalysis.createHtmlAnalysisFile(da, new File("climbData.html"));
    }

    private static void createFiles() throws IOException, JSONException {
        File file = new ClassPathResource("routesGermany400ml.json").getFile();

        JSONObject obj = new JSONObject(new String(Files.readAllBytes(file.toPath())));
        JSONArray routes = obj.getJSONArray("routes");
        List<List<Writable>> data = new ArrayList<>();


        IntStream.range(0, routes.length() - 1)
                .forEach((i) -> {
                    try {
                        JSONObject route = (JSONObject) routes.get(i);
                        JSONArray locations = (JSONArray) route.get("location");
                        IntStream.range(0, locations.length() - 1)
                                .forEach((k) -> {
                                    try {
                                        route.put("loc_" + k, locations.getString(k));
                                    } catch (Exception e) {
                                        System.out.println("Error getting location");
                                    }

                                });
                        File routeFile = new File(
                                "/Users/felipelopez/Documents/climbing-map/dl4j-examples/datavec-examples/src/main/resources/" +
                                        "climbData/routeLoc_0" + i + ".txt");
                        routeFile.createNewFile();
                        try (PrintWriter out = new PrintWriter(routeFile)) {
                            out.println(route.toString());
                        }
                    } catch (Exception e) {
                        System.out.println("Error: " + e.getLocalizedMessage());
                        e.printStackTrace();
                    }
                });
    }


}

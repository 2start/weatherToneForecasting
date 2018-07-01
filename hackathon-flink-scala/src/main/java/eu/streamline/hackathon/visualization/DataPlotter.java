package eu.streamline.hackathon.visualization;

import com.typesafe.config.ConfigException;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.paint.Color;
import javafx.stage.Stage;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class DataPlotter extends Application {

    final String csvFilePath = "all-events-weather-tone.csv";
    final String stageTitle = "Riot tone by Temperature";
    final String lineChartTitle = "General Tone by Temperature";

    @Override public void start(Stage stage) {

        ArrayList<String[]> csvData = new ArrayList<String[]>();

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(csvFilePath));
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                String[] lineData = line.split(",");
                csvData.add(lineData);
            }
        } catch (IOException e) { }

        stage.setTitle(stageTitle);

        //defining the axes
        final CategoryAxis xAxis = new CategoryAxis();
        final NumberAxis yAxis = new NumberAxis();
        xAxis.setLabel("Date");
        yAxis.setLabel("Tone");
        //creating the chart
        final LineChart<String,Number> lineChart =
                new LineChart<String,Number>(xAxis,yAxis);

        lineChart.setTitle(lineChartTitle);
        //defining a series
        XYChart.Series toneLow = new XYChart.Series();
        XYChart.Series toneMid = new XYChart.Series();
        XYChart.Series toneHigh = new XYChart.Series();
        toneLow.setName("Low temp");
        toneMid.setName("Mid temp");
        toneHigh.setName("High temp");

        //populating the series with data

        for (String[] row : csvData) {

            if(row[1].equals("low")) {
                toneLow.getData().add(new XYChart.Data(row[0], Double.valueOf(row[2])));
            } else if(row[1].equals("mid")){
                toneMid.getData().add(new XYChart.Data(row[0], Double.valueOf(row[2])));
            } else {
                toneHigh.getData().add(new XYChart.Data(row[0], Double.valueOf(row[2])));
            }
        }

        Scene scene  = new Scene(lineChart,800,600, Color.WHITE);

        lineChart.getData().add(toneLow);
        lineChart.getData().add(toneMid);
        lineChart.getData().add(toneHigh);

        toneLow.getNode().setStyle("-fx-stroke: #6cccff");
        toneMid.getNode().setStyle("-fx-stroke: #74e16d");
        toneHigh.getNode().setStyle("-fx-stroke: #ff464c");

        List<XYChart.Data<String, Number>> datapoints = toneLow.getData();
        for(XYChart.Data<String, Number> data : datapoints){
            data.getNode().setStyle("-fx-background-color: #6cccff, white");
        }

        datapoints = toneMid.getData();
        for(XYChart.Data<String, Number> data : datapoints){
            data.getNode().setStyle("-fx-background-color: #74e16d, white");
        }

        datapoints = toneHigh.getData();
        for(XYChart.Data<String, Number> data : datapoints){
            data.getNode().setStyle("-fx-background-color: #ff464c, white");
        }

        Platform.runLater(new Runnable() {
            @Override
            public void run() {
                Set<Node> nodes = lineChart.lookupAll(".chart-legend-item-symbol.default-color0");
                for (Node n : nodes) {
                    n.setStyle("-fx-background-color: #6cccff, white");
                }

                nodes = lineChart.lookupAll(".chart-legend-item-symbol.default-color1");
                for (Node n : nodes) {
                    n.setStyle("-fx-background-color: #74e16d, white");
                }

                nodes = lineChart.lookupAll(".chart-legend-item-symbol.default-color2");
                for (Node n : nodes) {
                    n.setStyle("-fx-background-color: #ff464c, white");
                }
            }
        });

        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
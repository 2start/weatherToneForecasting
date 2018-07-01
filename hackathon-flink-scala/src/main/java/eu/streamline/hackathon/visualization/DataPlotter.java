package eu.streamline.hackathon.visualization;

import javafx.application.Application;
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

public class DataPlotter extends Application {

    final String csvFilePath = "all-weather-mentions.csv";
    final String stageTitle = "General tone by Temperature";
    final String lineChartTitle = "Amount of Mentions by Precipitation";

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
        //creating the chart
        final LineChart<String,Number> lineChart =
                new LineChart<String,Number>(xAxis,yAxis);

        lineChart.setTitle(lineChartTitle);
        //defining a series
        XYChart.Series toneLow = new XYChart.Series();
        XYChart.Series toneMid = new XYChart.Series();
        XYChart.Series toneHigh = new XYChart.Series();
        toneLow.setName("toneLow");
        toneMid.setName("toneMid");
        toneHigh.setName("toneHigh");

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



        stage.setScene(scene);
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}
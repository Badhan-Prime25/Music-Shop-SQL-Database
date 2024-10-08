/*
 * Client.java
 *
 * A client for accessing the records database
 * A naive JavaFX for connecting to the database server and interact
 * with the database.
 *
 * author: 2614405
 *
 */


import java.util.List;
import java.util.ArrayList;

import java.lang.ClassNotFoundException;
import java.lang.IndexOutOfBoundsException;

import java.net.Socket;
import java.net.UnknownHostException;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.ObjectInputStream;

import java.sql.*;

import javax.sql.rowset.CachedRowSet;


import javafx.application.Application;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.control.TextArea;
import javafx.scene.control.TableView;
import javafx.scene.control.TableColumn;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.stage.Stage;
import javafx.geometry.Insets;
import javafx.collections.ObservableList;
import javafx.collections.FXCollections;
import javafx.beans.property.StringProperty;
import javafx.beans.property.SimpleStringProperty;



public class RecordsDatabaseClient extends Application {

	public static RecordsDatabaseClient me; //Get the application instance in javafx
	public static Stage thePrimaryStage;  //Get the application primary scene in javafx
	private Socket clientSocket = null;

	private String userCommand = null; //The user command
	private CachedRowSet serviceOutcome = null; //The service outcome


	//Convenient to populate the TableView
	public class MyTableRecord {
		private StringProperty title;
		private StringProperty label;
		private StringProperty genre;
		private StringProperty rrp;
		private StringProperty copyID;

		public void setTitle(String value) { titleProperty().set(value); }
		public String getTitle() { return titleProperty().get(); }
		public void setLabel(String value) { labelProperty().set(value); }
		public String getLabel() { return labelProperty().get(); }
		public void setGenre(String value) { genreProperty().set(value); }
		public String getGenre() { return genreProperty().get(); }
		public void setRrp(String value) { rrpProperty().set(value); }
		public String getRrp() { return rrpProperty().get(); }
		public void setCopyID(String value) { copyIDProperty().set(value); }
		public String getCopyID() { return copyIDProperty().get(); }


		public StringProperty titleProperty() {
			if (title == null)
				title = new SimpleStringProperty(this, "");
			return title;
		}
		public StringProperty labelProperty() {
			if (label == null)
				label = new SimpleStringProperty(this, "");
			return label;
		}
		public StringProperty genreProperty() {
			if (genre == null)
				genre = new SimpleStringProperty(this, "");
			return genre;
		}
		public StringProperty rrpProperty() {
			if (rrp == null)
				rrp = new SimpleStringProperty(this, "");
			return rrp;
		}
		public StringProperty copyIDProperty() {
			if (copyID == null)
				copyID = new SimpleStringProperty(this, "");
			return copyID;
		}

	}





	//Class Constructor
	public RecordsDatabaseClient(){

		me=this;
	}


	//Initializes the client socket using the credentials from class Credentials.
	public void initializeSocket(){

		//TO BE COMPLETED
		try {
			String host = Credentials.HOST;
			int port = Credentials.PORT;
			clientSocket = new Socket(host,port);
		} catch (IOException e) {
			System.out.println("Client: I/O error. " + e);
		}


	}


	public void requestService() {
		try {
			System.out.println("Client: Requesting records database service for user command \n" + this.userCommand +"\n");

			//TO BE COMPLETED
			OutputStream requestStream = this.clientSocket.getOutputStream();
			OutputStreamWriter streamWriter = new OutputStreamWriter(requestStream);
			streamWriter.write(this.userCommand + "#");
			streamWriter.flush();
		}catch(IOException e){
			System.out.println("Client: I/O error. " + e);

		}
	}



	public void reportServiceOutcome() {
		try {
			//TO BE COMPLETED
			InputStream inputStream = clientSocket.getInputStream();
			ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
			this.serviceOutcome = (CachedRowSet) objectInputStream.readObject();

			GridPane grid = (GridPane) thePrimaryStage.getScene().getRoot();
			ObservableList<Node> child = grid.getChildren();
			TableView<MyTableRecord> outputBox = (TableView<MyTableRecord>)child.get(5);


			ObservableList<MyTableRecord> recordsList =  FXCollections.observableArrayList();
			recordsList.clear();

			this.serviceOutcome.beforeFirst();
			String val = "\n";
			while(this.serviceOutcome.next()){
				MyTableRecord tmpRecords = new MyTableRecord();
				tmpRecords.setTitle(this.serviceOutcome.getObject("Title").toString());
				tmpRecords.setLabel(this.serviceOutcome.getObject("Label").toString());
				tmpRecords.setGenre(this.serviceOutcome.getObject("Genre").toString());
				tmpRecords.setRrp(this.serviceOutcome.getObject("RRP").toString());
				tmpRecords.setCopyID(this.serviceOutcome.getObject("NumCopies").toString());
				recordsList.add(tmpRecords);

				val =val+tmpRecords.getTitle()+"|"+tmpRecords.getLabel()+"|"+tmpRecords.getGenre()+"|"+tmpRecords.getRrp()+"|"+tmpRecords.getCopyID()+"\n";
			}
			outputBox.setItems(recordsList);
			outputBox.refresh();

			String tmp = "****"+recordsList;
			System.out.println(tmp +"\n"+val+"\n====================================\n");
		}catch(IOException e){
			System.out.println("Client: I/O error. " + e);
		}catch(ClassNotFoundException e){
			System.out.println("Client: Unable to cast read object to CachedRowSet. " + e);
		}catch(SQLException e){
			System.out.println("Client: Can't retrieve requested attribute from result set. " + e);
		}
	}

	//Execute client
	public void execute(){
		GridPane grid = (GridPane) thePrimaryStage.getScene().getRoot();
		ObservableList<Node> childrens = grid.getChildren();
		TextField artistInputBox = (TextField) childrens.get(1);
		TextField recordshopInputBox = (TextField) childrens.get(3);

		//Build user message command
		String surname = artistInputBox.getText();
		String city = recordshopInputBox.getText();
		this.userCommand = surname + ";" + city;

		//TO BE COMPLETED

		//Request service
		try{

			//Initializes the socket
			this.initializeSocket();

			//Request service
			this.requestService();

			//Report user outcome of service
			this.reportServiceOutcome();

			//Close the connection with the server
			this.clientSocket.close();

		}catch(Exception e)
		{// Raised if connection is refused or other technical issue
			System.out.println("Client: Exception " + e);
		}
	}




	@Override
	public void start(Stage primaryStage) {
		primaryStage.setTitle("Records Database Client");

		//Create a GridPane container
		GridPane grid = new GridPane();
		grid.setPadding(new Insets(10, 10, 10, 10));
		grid.setVgap(5);
		grid.setHgap(5);


		//Add the input boxes
		Label artistLabel = new Label("Artist's Surname:");
		GridPane.setConstraints(artistLabel, 0, 0);
		grid.getChildren().add(artistLabel);

		TextField artistInputBox = new TextField ();
		artistInputBox.setPromptText("Artist's Surname:");
		artistInputBox.setPrefColumnCount(30);
		GridPane.setConstraints(artistInputBox, 1, 0);
		grid.getChildren().add(artistInputBox);

		Label recordshopLabel = new Label("Record shop's city:");
		GridPane.setConstraints(recordshopLabel, 0, 1);
		grid.getChildren().add(recordshopLabel);

		TextField recordshopInputBox = new TextField ();
		recordshopInputBox.setPromptText("Record shop's city:");
		recordshopInputBox.setPrefColumnCount(30);
		GridPane.setConstraints(recordshopInputBox, 1, 1);
		grid.getChildren().add(recordshopInputBox);

		//Add the service request button
		Button btn = new Button();
		btn.setText("Request Records Database Service");
		btn.setOnAction(new EventHandler<ActionEvent>() {

			@Override
			public void handle(ActionEvent event) {
				me.execute();
			}
		});
		GridPane.setConstraints(btn, 0, 2, 2, 1);
		grid.getChildren().add(btn);

		//Add the output box
		TableView<MyTableRecord> outputBox = new TableView<MyTableRecord>();
		TableColumn<MyTableRecord,String> titleCol     = new TableColumn<MyTableRecord,String>("Title");
		TableColumn<MyTableRecord,String> labelCol = new TableColumn<MyTableRecord,String>("Label");
		TableColumn<MyTableRecord,String> genreCol     = new TableColumn<MyTableRecord,String>("Genre");
		TableColumn<MyTableRecord,String> rrpCol       = new TableColumn<MyTableRecord,String>("RRP");
		TableColumn<MyTableRecord,String> copyIDCol    = new TableColumn<MyTableRecord,String>("Num. Copies");
		titleCol.setCellValueFactory(new PropertyValueFactory("title"));
		labelCol.setCellValueFactory(new PropertyValueFactory("label"));
		genreCol.setCellValueFactory(new PropertyValueFactory("genre"));
		rrpCol.setCellValueFactory(new PropertyValueFactory("rrp"));
		copyIDCol.setCellValueFactory(new PropertyValueFactory("copyID"));


		@SuppressWarnings("unchecked") ObservableList<TableColumn<MyTableRecord,?>> tmp = outputBox.getColumns();
		tmp.addAll(titleCol, labelCol, genreCol, rrpCol, copyIDCol);
		//Leaving this type unchecked by now... It may be convenient to compile with -Xlint:unchecked for details.

		GridPane.setConstraints(outputBox, 0, 3, 2, 1);
		grid.getChildren().add(outputBox);

		//Adjust gridPane's columns width
		ColumnConstraints col1 = new ColumnConstraints();
		col1.setPercentWidth(25);
		ColumnConstraints col2 = new ColumnConstraints();
		col2.setPercentWidth(75);
		grid.getColumnConstraints().addAll( col1, col2);

		primaryStage.setScene(new Scene(grid, 505, 505));
		primaryStage.show();


		thePrimaryStage = primaryStage;


	}




	public static void main (String[] args) {
		launch(args);
		System.out.println("Client: Finished.");
		System.exit(0);
	}
}

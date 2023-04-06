package avro.example;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class ShowSerializer {

    public static void main(String[] args) throws IOException {

       Show show1 = new Show();
       show1.setName("Suits");
       show1.setYear(2011);
       show1.setRating(7.8);

       //Another method of creating a object
        Show show2 = new Show("GOT", 2011 ,9.0 );
        Show show3 = new Show("FROM", 2020 , 8.8) ;


        DatumWriter<Show> showDatumWriter = new SpecificDatumWriter<Show>(Show.class);
        DataFileWriter<Show> dataFileWriter = new DataFileWriter<Show>(showDatumWriter);
        dataFileWriter.create(show1.getSchema(), new File("target/avro-files/shows-file.txt"));
        dataFileWriter.append(show1);
        dataFileWriter.append(show2);
        dataFileWriter.append(show3);
        dataFileWriter.close();
        System.out.println("Serialization Completed");
    }
}

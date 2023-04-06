package avro.example;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

public class ShowDeserializer {

    public static void main(String[] args) throws IOException {

        File file = new File("target/avro-files/shows-file.txt");
        DatumReader<Show> showDatumReader = new SpecificDatumReader<Show>(Show.class);
        DataFileReader<Show> dataFileReader = new DataFileReader<Show>(file, showDatumReader);
        Show show = null;
        while (dataFileReader.hasNext()) {
            // Reuse show object by passing it to next(). This saves us from
            // allocating and garbage collecting many objects for files with
            // many items.
            show = dataFileReader.next(show);
            System.out.println("Show : "+show);
        }
    }
}
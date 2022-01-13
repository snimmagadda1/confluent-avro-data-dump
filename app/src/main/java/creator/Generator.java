package creator;

import java.io.File;
import java.time.LocalDate;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

@Slf4j
public class Generator {

  private static final String OUT_LOC = "people.avro";

  public static void main(String[] args) {
    try {
      // Parse + create the schema
      // Schema schema = new Schema.Parser().parse(new File(SCHEMA_LOC));
      ReflectData reflectData = ReflectData.get();
      reflectData.addLogicalTypeConversion(new TimeConversions.DateConversion());
      Schema schema = reflectData.getSchema(Person.class);

      log.info("Initialized schema");

      // Create write output
      File file = new File("data.avro");
      DatumWriter<Person> writer = new ReflectDatumWriter<Person>(schema);
      DataFileWriter<Person> dataFileWriter = new DataFileWriter<Person>(writer);
      dataFileWriter.create(schema, file);

      // Create records
      for (int i = 0; i < 100; i++) {
        Person person = new Person(generateName(), generateName(), LocalDate.of(2001, 1, 1));
        dataFileWriter.append(person);
      }

      dataFileWriter.close();
      log.info("Wrote avro to {}", OUT_LOC);

    } catch (Exception e) {
      log.error("Error", e);
    }
  }

  private static String generateName() {
    int leftLimit = 97;
    int rightLimit = 122;
    int targetStringLength = 10;
    Random random = new Random();

    StringBuilder buffer = new StringBuilder(targetStringLength);
    for (int i = 0; i < targetStringLength; i++) {
      int randomLimitedInt = leftLimit + random.nextInt(rightLimit - leftLimit + 1);
      buffer.append((char) randomLimitedInt);
    }
    String generatedString = buffer.toString();
    return generatedString;
  }
}

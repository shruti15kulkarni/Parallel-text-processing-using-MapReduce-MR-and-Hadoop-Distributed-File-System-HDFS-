import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileOutputStream;

public class ReadReplace{

	private static final String FILENAME = "vergil.aeneid.tess";

	public static void main(String[] args) {

		try (BufferedReader br = new BufferedReader(new FileReader(FILENAME))) 
		{
			 StringBuilder sb = new StringBuilder();
        String line = br.readLine();

        while (line != null) {
            sb.append(line);
            sb.append("\n");
            line = br.readLine();
        }
       String input = (sb.toString());
       input = input.replace('j', 'i');
        input = input.replace('J', 'I');
         input = input.replace('V', 'U');
       input = input.replace('v', 'u');
		
		FileOutputStream fileOut = new FileOutputStream(FILENAME);
        fileOut.write(input.getBytes());
        fileOut.close();


    

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
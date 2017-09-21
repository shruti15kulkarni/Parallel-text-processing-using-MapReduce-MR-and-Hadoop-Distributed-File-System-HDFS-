import java.util.*;  
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

public class Working{
public static void main(String[] args) throws IOException {
    String key;
    ArrayList<String> values;
    BufferedReader br = new BufferedReader(new FileReader("new_lemmatizer.csv"));
    String line =  null;
    Map<String, ArrayList<String>> hm = new HashMap<String, ArrayList<String>>();
    
    while((line=br.readLine())!=null){
    values = new ArrayList<String>();
    String str[] = line.split(",");
    key = str[0];
        for(int i=1;i<str.length;i++)
            {
            values.add(str[i]);
            }

            hm.put(key, values);
          }


// Adding locations to a key



BufferedReader br2 = new BufferedReader(new FileReader("lucan.bellum_civile.part.1.tess"));
String line3 =  null;
Map<String, ArrayList<String>> hm3 = new HashMap<String, ArrayList<String>>();
while((line3=br2.readLine())!=null){
    String strn[] = line3.split("\t");
    String valn = strn[0];
    String strn1[] = strn[1].split(" |\t|, |: |; |\\."); 
            for(int i=0;i<strn1.length;i++){
                strn1[i] = strn1[i].toLowerCase();
                strn1[i] = strn1[i].replace("?", "");
                strn1[i] = strn1[i].replace(",", "");
                strn1[i] = strn1[i].replace(":", "");
                strn1[i] = strn1[i].replace("-", "");
                ArrayList<String> listn;
                if(hm3.containsKey(strn1[i])){
                listn = hm3.get(strn1[i]);
                listn.add(valn);
                 }
                else 
                {
                 listn = new ArrayList<String>();
                listn.add(valn);
                hm3.put(strn1[i], listn);
                }
           
        }
}

BufferedReader br4 = new BufferedReader(new FileReader("vergil.aeneid.tess"));
String line4 =  null;

//Map<String, ArrayList<String>> hm4 = new HashMap<String, ArrayList<String>>();
while((line4=br4.readLine())!=null )
{
    if (line4.isEmpty() || line4.trim().equals("") || line4.trim().equals("\n")) 
           System.out.println(" ");
       else{
    String strg[] = line4.split("\t");
    String valg = strg[0];
   // System.out.println(valg);
    String strg1[] = strg[1].split(" "); 
            for(int i=0;i<strg1.length;i++){
                strg1[i] = strg1[i].toLowerCase();
                strg1[i] = strg1[i].replace("?", "");
                strg1[i] = strg1[i].replace(",", "");
                strg1[i] = strg1[i].replace(":", "");
                strg1[i] = strg1[i].replace("-", "");
                //System.out.println(strg1[i]);
                ArrayList<String> listg;
                if(hm3.containsKey(strg1[i])){
                listg = hm3.get(strg1[i]);
                listg.add(valg);
                    
                 }
                else 
                {
                 listg = new ArrayList<String>();
                listg.add(valg);
                hm3.put(strg1[i], listg);
                }
                
           
        }
       //System.out.println(valg);
   }     //ystem.out.println(count); 
        //count = count +1;
}
 
    PrintWriter writer = new PrintWriter("Latin_output.txt", "UTF-8");
    


    String line1 =  null;
    //Map<String, ArrayList<String>> hm1 = new HashMap<String, ArrayList<String>>();
    BufferedReader br1 = new BufferedReader(new FileReader("lucan.bellum_civile.part.1.tess"));
    while((line1=br1.readLine())!=null){
    String stri[] = line1.split("\t");
    String stri1[] = stri[1].split(" |\t|, |: |; |\\."); 
    for(int i=0;i<stri1.length;i++)
        {
        stri1[i] = stri1[i].toLowerCase();
        stri1[i] = stri1[i].replace("?", "");
        stri1[i] = stri1[i].replace(",", "");
        stri1[i] = stri1[i].replace(":", "");
        
        ArrayList<String> listi; 
        if(stri1[i] != null && !stri1[i].isEmpty()) {
            //System.out.println(stri1[i]);
        if(hm.containsKey(stri1[i]))
        {
                  
                  listi = hm.get(stri1[i]);
                  for (int j = 0; j < listi.size(); j++) 
                  {
                    if(hm3.containsKey(listi.get(j)))
                    {
                   writer.print(stri1[i] + "\t" + listi.get(j));
                    writer.print("\t");  
                    writer.println(hm3.get(listi.get(j)));
                        //System.out.println(hm3.get(listi.get(j)));
                    }
                  }
        }
        else
        {
                writer.println(stri1[i] + "\t" + hm3.get(stri1[i]));
            //System.out.println(stri1[i]);
        }
             
         }     //System.out.println(listi);
        
            //values.add(str[i]);
        }
             //System.out.println(values);
}
 //hm3.forEach((k, value) -> System.out.println(k + ", " + value));
 //hm.forEach((k, value) -> System.out.println(k + ", " + value));
 // System.out.println(hm);
    }
   
 }






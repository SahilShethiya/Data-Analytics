package mypkg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class convertResult {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File f=new File(args[0]);
		ArrayList<String> ar=new ArrayList<String>();
		
		try {
			BufferedReader br=new BufferedReader(new FileReader(f));
			String st;
			try {
				while((st=br.readLine())!= null) {
					String[] str=st.split("\\r?\\t");
					//System.out.println(str[1]);
					
					//if there are more positive words, number will be positive
					if(Integer.parseInt(str[1])>=0) {
						str[1]="positive";
					}
					
					//if there are more negative words, number will be less than zero
					else {
						str[1]="negative";
					}
					String ss=str[0] + "\t" +str[1];
					//System.out.println(ss);
					ar.add(ss);
					
				}
				br.close();
			}
			catch(IOException e) {
					System.out.println(e);
			}
			
		}
		catch(FileNotFoundException fe) {
			System.out.println(fe);
		}
	
		FileWriter writer = new FileWriter(args[1]); 
		for(String str: ar) {
		  writer.write(str + System.lineSeparator());
		}
		writer.close();

	}

}

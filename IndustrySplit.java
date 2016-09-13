package industry;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import general.Read;

public class IndustrySplit {
	static File industryInput = new File("industries.csv");
	static File dataInput = new File("output.csv");

	static String prefix = "output_";

	Map<String, String> industries;
	Map<String, List<String>> data;
	String header;
	public IndustrySplit() {}

	public static void main(String[] args) {
		IndustrySplit foo = new IndustrySplit();
		
		System.out.println("gathering industries");
		foo.industries = Read.readCSVtoMap(industryInput);
		
		System.out.println("gathering data");
		//?????????????????
		//foo.data = Read.readGatheredData(dataInput);
		//foo.header = Read.getFirstLine(dataInput);
		
		System.out.println("exporting");
		foo.export();
		
		System.out.println("All Done!");
	}
	public void export(){
		//for each industry
		Set<String> industrySet = new HashSet<String>(industries.values());
		for(String industry : industrySet){
			System.out.println(industry);
			File f = new File(prefix + industry + ".csv");
			try {
				BufferedWriter write = new BufferedWriter(new FileWriter(f));
				write.write(header+"\n");
				
				//for each company
				for(String security:industries.keySet()){
					if(industries.get(security).equals(industry)){
						//for each time period
						if(!data.containsKey(security))
							System.out.println("missing all data for: " + security);
						else{
							for(String line : data.get(security)){
								//System.out.println(line);
								write.write(line+ "\n");
							}
						}
					}
				}
				write.close();
			} catch (IOException e) {
				System.out.println("error in exporting: " + industry);
				e.printStackTrace();
			}
		}
	}
}
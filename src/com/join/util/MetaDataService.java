package com.join.util;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.join.bean.Table;

public class MetaDataService {
	
	public static String filename = "src/metadata.xml";

	public static Table[] loadMetadata() {
		Table[] tables = null;
		try {  
            File f = new File(filename);  
            if (!f.exists()) {  
                System.out.println("  Error : Config file doesn't exist!");  
                System.exit(1);  
            }  
            SAXReader reader = new SAXReader();  
            Document doc;  
            doc = reader.read(f);  
            Element root = doc.getRootElement();  
            Element data;  
            List lists = root.selectNodes("table"); 
            tables = new Table[lists.size()];
            int cnt = 0;
            for(Iterator itr = lists.iterator(); itr.hasNext(); ) {
            	data = (Element) itr.next();  
            	Table table = new Table();
            	List list = data.selectNodes("field");
            	int i = 0;
            	for(Iterator iterator = list.iterator(); iterator.hasNext(); ) {
            		table.getField().put(((Element)iterator.next()).getText().trim(), i ++);
            	}
            	table.setName(data.elementText("name".trim()));
	            table.setPath(data.elementText("path".trim())); 
	            tables[cnt++] = table;
            }
  
        } catch (Exception ex) {  
            System.out.println("Error : " + ex.toString());  
        }  
		return tables;
	}
	
}

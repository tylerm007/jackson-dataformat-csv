package com.espressologic.file.csv;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class CSVParserToJSON {

	private static char COLUMN_SEP = '\t';// or '\t' or ';' or '|' etc

	private String keyAttrName = "idMasterMapper";
	private String altKeyAttrName = "altForeignKey";
	// col1 = origColName col2 = replacement column
	private Map<String, String> mappedColumns = new HashMap<String, String>();
	private static String dir = "C:\\projects\\newlife\\reports\\";
	private HashMap<Integer,String> records = new HashMap<Integer,String>();
	/*
	 * if(row.processCSVFlag && row.content !== nulll){ var csv = new
	 * com.espressologic.file.csv.CSVParserToJSON("keycolumn"); var result =
	 * csv.CSVParserToJSON.convertFileToJSON(row.idvendor_pricelist ,
	 * row.vendorID,row.content); var json = JSON.parse(result);
	 * log.debug(result); }
	 * 
	 * //can also pass HashMap<String,String> map with oldColName,newColName in
	 * constructor new CSVParserToJSON("keycolumn", map);
	 */
	public CSVParserToJSON() {

	}
	public CSVParserToJSON(String keyName) {
		this.keyAttrName = keyName;
	}
	public CSVParserToJSON(String keyName, String altKeyName) {
		this.keyAttrName = keyName;
		this.altKeyAttrName = altKeyName;
	}
	public CSVParserToJSON(String keyName, Map<String, String> columnMap) {
		this.keyAttrName = keyName;
		this.setMappedColumns(columnMap);
	}
	public CSVParserToJSON(String keyName, String altKeyName,
			Map<String, String> columnMap) {
		this.keyAttrName = keyName;
		this.altKeyAttrName = altKeyName;
		this.setMappedColumns(columnMap);
	}

	public static void main(String[] args) {
		//testcsv.csv,
		
		File file = new File(dir + "Daily Inventory History.txt");
		byte[] bFile = new byte[(int) file.length()];
		try {
			FileInputStream fileInputStream = new FileInputStream(file);
			fileInputStream.read(bFile);
			fileInputStream.close();

			for (int i = 0; i < bFile.length; i++) {
				// System.out.print((char) bFile[i]);
			}
			CSVParserToJSON csv = new CSVParserToJSON("idMasterMapper",
					"AttrDataType");
			CSVParserToJSON.setCOLUMN_SEP("\t");
			csv.addColumnMap("ASIN", "asin");
			String str = csv.convertFileToJSON(1, 1, bFile);
			System.out.println(csv.records.keySet().size());
			System.out.println(csv.records.get(new Integer(1)));
			System.out.println(csv.getBatch(1,20));
			//System.out.println(str);
			//String str2 = csv.convertFileHeaderToJSON(1 , 1, bFile);
			//System.out.println(str2);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public String convertFileToJSON(String keyName, int idvendor_pricelist,
			int altForeignKey, byte[] bytes) {
		keyAttrName = keyName;
		return convertFileToJSON(idvendor_pricelist, altForeignKey, bytes);
	}
	
	/**
	 * 
	 * @param startRow
	 * @param batchSize
	 * @return
	 */
	public String getBatch(int startRow, int batchSize){
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		int rowCount = 0;
		String sep = "";
		if(startRow > 0  && startRow <= records.size()){
			for(int i = startRow; i <= records.size(); i++){
				sb.append(sep);
				sb.append(records.get(new Integer(i)));
				sep = ",";
				rowCount++;
				if(rowCount == batchSize) break;
			}
		}
		sb.append("]");
		return sb.toString();
	}
	
	public int getTotalRecordCount(){
		return records.size();
	}
	/**
	 * Make sure your file is saved as UTF-8 (windows editors will convert to
	 * DOS ANSI and you will get errors)
	 * 
	 * @param idKey1
	 * @param idKey2
	 * @param bytes
	 * @return
	 */
	public String convertFileToJSON(int idKey1, int idKey2, byte[] bytes) {

		CsvMapper mapper = new CsvMapper();

		// important: we need "array wrapping" (see next section) here:
		mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		// CsvSchema schema = CsvSchema.emptySchema().withHeader();

		MappingIterator<String[]> it;
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			mapper.schemaFor(String[].class).withColumnSeparator(COLUMN_SEP)
					.withHeader().rebuild();
			CsvSchema schema = mapper.schemaFor(String[].class).withoutHeader()
					.withColumnSeparator(COLUMN_SEP);
			it = mapper.reader(String[].class).with(schema).readValues(bis);
			String[] row = null;
			String[] columnHeader = null;
			StringBuffer rowSB;
			int rownum = 0;
			String sep = "";
			String rowSep = "";
			while (it.hasNextValue()) {
				row = it.nextValue();
				rowSB = new StringBuffer();
				// and voila, column values in an array. Works with Lists as
				if (rownum == 0) {
					// this is our first row so skip it
					columnHeader = row.clone();

				} else {
					String cs = ",";
					
					// to do replace with Class
					for (int i = 0; i < row.length; i++) {
						// skip blank and null rows
						if (row[i] != null && !"".equals(row[i])) {
							rowSB.append(sep);
							rowSB.append("{");
							format(rowSB, "\"" + keyAttrName + "\":",
									String.valueOf(idKey1));
							rowSB.append(cs);
							format(rowSB, "\"row\":", String.valueOf(rownum));
							rowSB.append(cs);
							format(rowSB, "\"columnName\":",
									quote(replaceColumnName(columnHeader[i])));
							rowSB.append(cs);
							format(rowSB, "\"value\":", quote(row[i]));
							if (idKey2 > 0) {
								rowSB.append(cs);
								format(rowSB, "\"" + altKeyAttrName + "\":",
										String.valueOf(idKey2));
							}
							rowSB.append("}");
							sep = ",";
							
						}
					}
				}
				
				System.out.print(".");
				if(rownum > 0) {
					records.put(new Integer(rownum), rowSB.toString());
					sb.append(rowSep);
					sb.append(rowSB.toString());
					rowSep = ",";
					sep = "";
				}
				rowSB = new StringBuffer();
				
				rownum++;
			}
			
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sb.append("]");
		System.out.println("done");
		return sb.toString();
	}

	public String convertFileHeaderToJSON(int idKey1, int idKey2, byte[] bytes) {

		CsvMapper mapper = new CsvMapper();

		// important: we need "array wrapping" (see next section) here:
		mapper.enable(CsvParser.Feature.WRAP_AS_ARRAY);
		// need to create a record [{ vendorID, row, columnName, value}]
		MappingIterator<String[]> it;
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			mapper.schemaFor(String[].class).withColumnSeparator(COLUMN_SEP)
					.rebuild();
			CsvSchema schema = mapper.schemaFor(String[].class).withoutHeader()
					.withColumnSeparator(COLUMN_SEP);
			it = mapper.reader(String[].class).with(schema).readValues(bis);
			String[] row = null;
			String[] columnHeader = null;

			int rownum = 0;
			String sep = "";
			while (it.hasNextValue()) {
				row = it.nextValue();
				// and voila, column values in an array. Works with Lists as
				if (rownum == 0) {
					// this is our first row so skip it
					columnHeader = row.clone();
					String cs = ",";

					for (int i = 0; i < columnHeader.length; i++) {
						// skip blank and null rows
						if (row[i] != null && !"".equals(row[i])) {
							sb.append(sep);
							sb.append("{");
							format(sb, "\"" + keyAttrName + "\":",
									String.valueOf(idKey1));

							sb.append(cs);
							format(sb, "\"CSVAttrName\":",
									quote((columnHeader[i])));
							sb.append(cs);
							format(sb, "\"TableAttrName\":",
									quote(modifyColumn(columnHeader[i])));
							sb.append("}");
							sep = ",";
						}
					}
				}
				rownum++;
				break;
			}

		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sb.append("]");
		return sb.toString();
	}

	private String replaceColumnName(String currColName) {
		String response = currColName;
		// use the map to find and swap the name
		for (String mappedColName : mappedColumns.keySet()) {
			if (response.equalsIgnoreCase(mappedColName)) {
				response = mappedColumns.get(mappedColName);
				break;
			}
		}
		return response;
	}
	private String quote(String value) {
		String val = (value == null) ? "" : value.replaceAll("\"", "'");
		return "\"" + val + "\"";
	}
	/**
	 * Modify this if your columns have weird charcters
	 * 
	 * @param csvAttrName
	 * @return
	 */
	private String modifyColumn(String csvAttrName) {
		String colName = new String(csvAttrName).replaceAll("-", "_");
		colName = colName.replaceAll("$", "");
		colName = colName.replaceAll("\\((.+?)\\)", "");
		return colName.replaceAll(" ", "_").toLowerCase();
	}

	private void format(StringBuffer sb, String key, String value) {
		sb.append(key);
		sb.append(value);
	}
	public String getKeyAttrName() {
		return keyAttrName;
	}
	public void setKeyAttrName(String keyAttrName) {
		this.keyAttrName = keyAttrName;
	}
	public String getAltKeyAttrName() {
		return altKeyAttrName;
	}
	public void setAltKeyAttrName(String altKeyAttrName) {
		this.altKeyAttrName = altKeyAttrName;
	}
	public Map<String, String> getMappedColumns() {
		return mappedColumns;
	}
	public void setMappedColumns(Map<String, String> mappedColumns) {
		this.mappedColumns = mappedColumns;
	}
	public void addColumnMap(String oldColName, String newColName) {
		this.mappedColumns.put(oldColName, newColName);
	}
	public static char getCOLUMN_SEP() {
		return COLUMN_SEP;
	}
	public static void setCOLUMN_SEP(char separator) {
		COLUMN_SEP = separator;
	}
	public static void setCOLUMN_SEP(String separator) {
		COLUMN_SEP = "\\t".equals(separator)?'\t':separator.charAt(0);
	}
	//test the data type of the input and guess the transform value		
	public static boolean isNumber(String test){return false;}
	public static boolean isBoolean(String test){return false;}
	public static boolean isDate(String test){return false;}
	public static boolean isMoney(String test){return false;}
}

   ```
   //This is sample code to use in Espresso Logic EVENT logic 
   
   //READ the mapping table
    var data = SysUtility.getResource("mapping",{pagesize: 25});
    log.debug(data);
   // pass in the name of the primary and alkey field names
    var csvPraser = new com.espressologic.file.csv.CSVParserToJSON("idScanPowerRequest","vendorID");
    if (Array.isArray(data)) {
        for (var i = 0; i < data.length; i++) {
            csvPraser.addColumnMap(data[i].scanPowerAttrName,data[i].nlvPricingAttrName);
        }
    }
    //the row.RequestCSV is an input stream of CSV - make sure there are not funny characters in the file
    var result = csvPraser.convertFileToJSON(row.idScanPowerRequest,row.VendorID,row.RequestCSV);
    var json = JSON.parse(result);
    log.debug(result);
    var url = req.baseURL+"main%3Adetails";
  
    log.debug(url);
    var parms = null;
    var settings = {
        headers: {"Authorization": "Espresso {yourapikey}:123"}
    };
    //write the CSV file out to a details table name/value pairs
    var post = SysUtility.restPost(url,parms,settings,json);
 
    log.debug(post);
```
